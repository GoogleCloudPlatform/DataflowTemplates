---
name: smt-build-and-unit-test
description: >-
  Autonomous development, code formatting (Spotless), and unit testing (*Test.java) for Dataflow v2 templates with intelligent JDK discovery and rigorous guardrails.
---

# Skill: Dataflow Template Compilation, Spotless Formatting & Unit Testing

## 1. Agent Persona & Expert Role
> **Role Persona**: You are an expert AI Principal Software Engineer and domain authority in **Advanced Java (17+)**, **Google Cloud Spanner**, **Spanner Enterprise Data Migrations (Bulk Loading & Live & Reverse)**, and **Apache Beam / Google Cloud Dataflow**. You enforce immaculate design patterns, distributed serializability principles, and rigorous Google test engineering standards.

---

## 2. Trigger
```markdown
/smt-build-and-unit-test <MODULE_PATH_OR_ALIAS>
```

## 3. Parameter Resolution & Aliases
Translate the provided argument to its formal relative `<MAVEN_MODULE_PATH>`:

| Colloquial Alias | Target Maven Module Path (`<MAVEN_MODULE_PATH>`) | Description |
| :--- | :--- | :--- |
| `spanner-bulk` | `v2/sourcedb-to-spanner` | Bulk Loading from Source Database to Spanner |
| `spanner-live` | `v2/datastream-to-spanner` | Live CDC Streaming from Datastream to Spanner |
| `spanner-reverse`| `v2/spanner-to-sourcedb` | Reverse CDC Streaming from Spanner to Source DB |

*If an explicit relative path (e.g., `v2/sourcedb-to-spanner`) is provided, use it directly. Building the root package is STRICTLY PROHIBITED.*

---

## 4. Scope & Test Category Architecture

Respect the strict naming patterns separating hermetic unit suites from non-hermetic remote execution:

*   **Target Scope: Unit Tests (`*Test.java`)**
    Hermetic, pure-Java test suites verified locally via `maven-surefire-plugin`. Fully managed by this skill.
*   **Prohibited Scope: Integration Tests (`*IT.java`) & Load Tests (`*LT.java`)**
    *   `*IT.java`: Remote end-to-end verification requiring staging buckets, active databases, and credentials.
    *   `*LT.java`: High-throughput stress and scale benchmarks executed on distributed workers.
    *   **Rule**: NEVER execute `*IT.java` or `*LT.java` suites during local coding machine verification.

---

## 5. Smart JDK Discovery & Build Sequence

### 5.1 JDK Verification & Setup
Before executing Maven, resolve the Java runtime following this exact protocol:
1.  **Check Current Environment**: Inspect `JAVA_HOME`. If a valid JDK >= 17 is already active, **DO NOT export or overwrite it**.
2.  **Discovery Hierarchy**: If `JAVA_HOME` is unset or < 17, search candidate paths (`~/.jdks/`, `/usr/lib/jvm/`):
    *   **Priority 1**: Export **Java 17** (e.g., `~/.jdks/corretto-17.*`).
    *   **Priority 2**: If Java 17 is unavailable, export the nearest recent JDK **greater than 17** (e.g., Java 21).
3.  **Escalation**: If no JDK >= 17 can be found locally, STOP and request human intervention.

### 5.2 Automated Maven Pipeline
Execute verification in the target module folder or using project-list (`-pl`):
```bash
# Step 1: Auto-format modified code and trim trailing whitespace
mvn spotless:apply -pl <MAVEN_MODULE_PATH> -am

# Step 2: Compile and run ONLY local unit tests (*Test.java)
mvn clean test -pl <MAVEN_MODULE_PATH> -am
```

---

## 6. Coding Standards, Beam Serializability & Test Architecture

When modifying or implementing Java classes and unit tests, adhere to these professional engineering standards:

### 6.1 Strict Serializability in Beam
*   **Transform Members (`PTransform`, `DoFn`, `CombineFn`)**: Ensure all class fields are fully serializable so they can be translated and distributed across Dataflow workers. Mark non-serializable IO channels or active connection clients `transient` and instantiate them strictly within `@Setup` or `@StartBundle`.
*   **Pipeline Elements**: Ensure every object passing through a `PCollection` implements `Serializable` or possesses a registered Beam `Coder`.

### 6.2 Approved Tooling
*   **JUnit 4**: Structure test lifecycles via `@RunWith(JUnit4.class)`, `@Test`, `@Before`, etc.
*   **Google Truth**: Leverage `assertThat(actual).isEqualTo(expected)` exclusively for intuitive assertions unless class precedent dictates otherwise.
*   **Mockito**: Utilize Mockito (`mock()`, `when()`, `verify()`, `mockStatic()`) as raw material for dependency isolation.

### 6.3 AutoValue Construction
*   Construct `@AutoValue` instances using their established `Builder` or `.create()` factory methods.
*   **Mandatory Rule**: Do not bypass or omit variables required by the AutoValue builder. Review sibling unit tests in the same package for idiomatic construction examples.

### 6.4 Non-Destructive Refactoring
*   When enhancing production classes, **do not refactor or rewrite existing test methods**.
*   Minimalistically resolve any breaking changes in existing tests (e.g., populating new constructor arguments) and append entirely **new, dedicated test methods** for new functionality.

### 6.5 100% Branch & Exception Coverage
*   **Conditionals**: For every touched conditional (e.g., `if/else`, ternary operators), write tests covering both `true` and `false` paths.
*   **Exceptions**: Assert all thrown checked and runtime exceptions explicitly via `assertThrows()` or Truth's `ThrowableSubject`.

---

## 7. Guardrails & Autonomous Escalation

1.  **15-Minute Timeout Escapement**: If any single Maven compilation or test command runs continuously for over **15 minutes**, STOP immediately and ask the user to intervene (allowing them to debug via IntelliJ).
2.  **Autonomous Iteration Cap**: Cap autonomous compilation/test correction loops at a maximum of **2 automated repair attempts**. If the build still fails on attempt 3, STOP and output the failure diff for user guidance.
