# Ingestion Worker — Docker Debugging Guide

This document explains **every problem** we hit while getting the ingestion-worker
container to start, **why** it happened, and **how** we fixed it.

---

## The Big Picture

Our Dockerfile has **two stages**:

```
Stage 1 (builder)                Stage 2 (runtime)
┌─────────────────┐             ┌──────────────────────────┐
│ maven:3.9 image │──► JAR ───►│ tabulario/spark-iceberg   │
│ Compiles Java   │             │ Has Spark + Iceberg jars  │
└─────────────────┘             │ Runs spark-submit         │
                                └──────────────────────────┘
```

The **builder** stage worked fine. All the problems were in **Stage 2** — the
runtime image — because the `tabulario/spark-iceberg` base image has a
non-standard user setup.

---

## Problem 1: Ivy crash — `basedir must be absolute: ?/.ivy2/local`

### What happened

When `spark-submit` starts, it uses **Apache Ivy** to resolve any extra
dependencies. Ivy looks for its local cache at:

```
${user.home}/.ivy2/local
```

Java reads `user.home` from the OS — specifically from `/etc/passwd`. But in
the `tabulario/spark-iceberg` image, the process runs as **uid 185**, and
that uid has **no entry** in `/etc/passwd`.

So Java returns `?` as the home directory:

```
user.home = "?"          ← not an absolute path!
Ivy path  = "?/.ivy2"   ← Ivy crashes: "basedir must be absolute"
```

### Why the first fix didn't work

Originally the Dockerfile used an inline `RUN echo '...\n...\n...'` to
generate the entrypoint script. The `\n` characters were interpreted
differently depending on the shell, and the `export HOME=...` line
sometimes ended up as a literal string instead of a real command.

### The fix

**1) Use a real file** instead of generating the script with `echo`:

```dockerfile
# Before (fragile)
RUN echo '#!/bin/bash\nexport HOME=/opt/spark/work-dir\n...' > entrypoint.sh

# After (reliable)
COPY entrypoint.sh ./entrypoint.sh
```

**2) Set `HOME` and `user.home` explicitly** in the entrypoint:

```bash
# entrypoint.sh
export HOME=/opt/spark/work-dir
```

```bash
# spark-submit flag
--driver-java-options "-Duser.home=/opt/spark/work-dir"
```

**3) Tell spark-submit where Ivy should live**:

```bash
--conf spark.jars.ivy=/opt/spark/work-dir/.ivy2
```

---

## Problem 2: Hadoop crash — `KerberosAuthException: invalid null input: name`

### What happened

After fixing Ivy, Spark started but crashed at a different point:

```
javax.security.auth.login.LoginException:
  javax.security.auth.callback.UnsupportedCallbackException:
    Invalid null input: name
```

This comes from **Hadoop's `UserGroupInformation`** class. When Spark
creates a `SparkContext`, Hadoop tries to figure out "who is the current
OS user?" by calling:

```java
System.getProperty("user.name")   // Java reads this from /etc/passwd
```

Since uid 185 has **no `/etc/passwd` entry**, Java returns `null`, and
Hadoop's Kerberos login module crashes because it can't handle a null
username.

### The chain

```
SparkContext.<init>
  └─► Utils.getCurrentUserName()
        └─► UserGroupInformation.getCurrentUser()
              └─► UserGroupInformation.getLoginUser()
                    └─► LoginContext.login()
                          └─► CRASH: "invalid null input: name"
```

### The fix

**1) Add the user to `/etc/passwd`** so Java can look up uid 185:

```dockerfile
RUN echo "spark:x:185:0:Spark User:/opt/spark/work-dir:/bin/bash" >> /etc/passwd
```

This line means:
| Field | Value | Meaning |
|-----------|--------------------------|---------------------------------|
| username | `spark` | the name Java will find |
| password | `x` | placeholder (unused) |
| uid | `185` | matches the container user |
| gid | `0` | root group (standard in Docker) |
| comment | `Spark User` | human-readable label |
| home dir | `/opt/spark/work-dir` | used by Java as `user.home` |
| shell | `/bin/bash` | default shell |

**2) Set `HADOOP_USER_NAME`** as a belt-and-suspenders approach:

```dockerfile
ENV HADOOP_USER_NAME=spark
```

Hadoop checks this env var **first** before doing the OS lookup. If it's
set, Hadoop uses it directly and never calls the Kerberos login code.

**3) Pass `-Duser.name=spark`** to the JVM:

```bash
--driver-java-options "-Duser.name=spark -Duser.home=/opt/spark/work-dir"
```

This overrides `System.getProperty("user.name")` at the Java level.

---

## Problem 3: Permission denied — `/home/iceberg/spark-events/`

### What happened

After fixing the user identity, Spark started the `SparkContext` but then
crashed writing to the **event log**:

```
java.io.FileNotFoundException:
  /home/iceberg/spark-events/local-1771676757400.inprogress
  (Permission denied)
```

The `tabulario/spark-iceberg` base image configures Spark's event log
directory at `/home/iceberg/spark-events/`. That directory is owned by
the `iceberg` user (from the base image), but our process runs as uid 185
(`spark`), which doesn't have write permission.

### The fix

Make that directory writable by uid 185:

```dockerfile
RUN mkdir -p /home/iceberg/spark-events && \
    chown -R 185:0 /home/iceberg/spark-events
```

---

## Problem 4: Iceberg REST Catalog health check — `curl not found`

### What happened (in docker-compose.yml, not the Dockerfile)

The `iceberg-rest-catalog` container was marked **unhealthy** because Docker
couldn't run the health check:

```yaml
healthcheck:
  test: ["CMD", "curl", "-f", "http://localhost:8181/v1/config"]
```

The `tabulario/iceberg-rest` image doesn't have `curl` installed. We also
tried `wget` — also not installed.

### The fix

Use **bash's built-in TCP check** instead:

```yaml
healthcheck:
  test: ["CMD", "bash", "-c", "exec 3<>/dev/tcp/localhost/8181"]
```

This opens a TCP connection to port 8181. If the connection succeeds (exit
code 0), the container is healthy. No external tools needed — bash handles
it natively with its `/dev/tcp` pseudo-filesystem.

> Note: this only works with `bash`, not `sh` (dash). That's why we use
> `["CMD", "bash", "-c", "..."]` and not `["CMD-SHELL", "..."]`.

---

## Summary of all Dockerfile changes

```dockerfile
# ① Add /etc/passwd entry for uid 185 → fixes Hadoop null username
RUN echo "spark:x:185:0:Spark User:/opt/spark/work-dir:/bin/bash" >> /etc/passwd && \
    # ② Create writable Ivy cache dir → fixes "basedir must be absolute"
    mkdir -p /opt/spark/work-dir/.ivy2 && \
    chown -R 185:0 /opt/spark/work-dir && \
    # ③ Make event log dir writable → fixes "Permission denied"
    mkdir -p /home/iceberg/spark-events && \
    chown -R 185:0 /home/iceberg/spark-events

# ④ Tell Hadoop the username via env var (backup for /etc/passwd)
ENV HADOOP_USER_NAME=spark
```

And in `entrypoint.sh`:

```bash
# ⑤ Set HOME for Ivy resolution
export HOME=/opt/spark/work-dir

# ⑥ JVM system properties as a final safety net
--driver-java-options "-Duser.name=spark -Duser.home=/opt/spark/work-dir"

# ⑦ Explicit Ivy cache location
--conf spark.jars.ivy=/opt/spark/work-dir/.ivy2
```

---

## Why did all of this happen?

One root cause: **the `tabulario/spark-iceberg` base image runs as uid 185
but doesn't give that user a proper identity.**

In a normal Linux system, every user has an entry in `/etc/passwd`. But
Docker images sometimes create users with `useradd --no-create-home` or
just use a raw `USER 185` directive — which sets the UID but doesn't add
it to `/etc/passwd`.

Java, Hadoop, and Ivy all assume they can look up the current user from
the OS. When they can't, they crash in different ways at different points
in the startup sequence.
