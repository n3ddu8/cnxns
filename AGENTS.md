# AGENTS.md

## Purpose

This document defines **durable architectural guidance** for any LLM coding assistant contributing to this library.

The library’s mission is to **simplify interaction with remote data systems** through a minimal, stable user-facing API (`cnxn`, `read`, `write`) while supporting a wide and evolving ecosystem of backends, data formats, and consumers.

This file is **not** an implementation plan.  
It is a set of **principles, constraints, and reasoning rules** that must guide all design and implementation work across many small, incremental changes.

---

## Role of the LLM Agent

When working on this repository, you are acting as:

- A **senior Python library architect**
- A **data engineering generalist**, not a framework specialist
- A **custodian of long-term architectural coherence**

Your responsibility is not only to “make it work”, but to ensure that each change:
- Reinforces the architectural direction
- Does not introduce unnecessary coupling
- Preserves API simplicity and stability

You must actively resist short-term convenience that undermines long-term maintainability.

---

## Core Architectural Principles

### 1. Data Representation Comes First

**The source of truth is data representation, not data consumers.**

- Internal data should be represented using **neutral, low-level abstractions**, such as:
  - Iterators / generators
  - Arrow tables or record batches
  - Row-oriented mappings
  - Byte or file-like streams
- Higher-level dataframes (e.g. Pandas, Polars, Spark) are **derived views**, never foundational structures.
- Do not assume:
  - In-memory materialisation
  - Columnar vs row-based layouts
  - Eager vs lazy execution

If a design decision implicitly assumes a dataframe, it is likely incorrect.

---

### 2. Dataframe Libraries Are Adapters, Not Dependencies

- Treat dataframe ecosystems as **optional adapters** layered on top of core data streams.
- The core library must remain usable without importing any dataframe library.
- Any integration with a dataframe framework should:
  - Live behind a clear adapter boundary
  - Be optional or provided via extras
  - Avoid infecting core types, APIs, or assumptions

The library must never require users to adopt a specific dataframe implementation.

---

### 3. Backend-Agnostic Design

Do **not** anchor designs to specific systems, protocols, or vendors.

- You may reference systems (e.g. SQL databases, APIs, object storage) **only as illustrative examples**.
- Avoid backend-specific concepts leaking into:
  - Public APIs
  - Core abstractions
  - Shared interfaces

Backends must conform to the library’s abstractions — not the other way around.

---

### 4. Capability-Based Interfaces

Backends vary widely in what they can support. Do not force them into a false uniformity.

- Prefer **capability-based design** over rigid inheritance hierarchies.
- Examples of capabilities (illustrative, not exhaustive):
  - Supports streaming reads
  - Supports chunked iteration
  - Supports predicate pushdown
  - Supports projection
  - Supports writes or append semantics
- Capabilities should be:
  - Discoverable
  - Explicit
  - Used to guide behaviour, not hidden behind magic

Never assume a backend can do something unless it explicitly advertises that capability.

---

### 5. Chunking and Streaming Are First-Class

Assume **large datasets by default**.

- Designs must support:
  - Incremental consumption
  - Bounded memory usage
  - Deferred or lazy evaluation where appropriate
- Chunking and streaming must:
  - Be abstract
  - Be expressed via iteration semantics
  - Avoid backend- or framework-specific configuration flags in public APIs

Materialisation should always be a **choice**, not a requirement.

---

### 6. Separation of Concerns

Maintain clear boundaries between:

- **Transport** (how data is fetched or sent)
- **Representation** (how data is structured in memory or streams)
- **Consumption** (how users choose to view or process data)

Avoid designs where:
- Transport decisions dictate representation
- Representation dictates consumption
- Consumption dictates transport

Violations of this separation tend to become permanent technical debt.

---

### 7. Minimal, Stable Public API

The public API is intentionally small and must remain so.

- Preserve the conceptual simplicity of:
  - `cnxn`
  - `read`
  - `write`
- Do not expose:
  - Backend-specific options
  - Framework-specific knobs
  - Execution details that users should not need to understand
- Internal complexity is acceptable.
- User-facing complexity is not.

Backward compatibility must be considered unless explicitly instructed otherwise.

---

### 8. Dependency Minimisation

- Avoid introducing heavy dependencies unless strictly necessary.
- Prefer:
  - The Python standard library
  - Small, focused libraries
  - Optional dependencies via extras
- Core logic must remain viable with minimal foundations.

Every new dependency should be defensible in terms of:
- Architectural necessity
- Maintenance burden
- Ecosystem stability

---

## How to Reason About Changes

When proposing or implementing a change, you should explicitly consider:

1. **Does this assume a specific backend or consumer?**
2. **Does this force eager materialisation?**
3. **Does this make streaming harder or impossible?**
4. **Does this introduce unnecessary coupling or dependencies?**
5. **Is this behaviour better expressed as a capability?**
6. **Will this scale to new backends and consumers we haven’t thought of yet?**

If the answer to any of these raises concern, call it out.

---

## Trade-offs and Feasibility

You are expected to:

- Identify trade-offs explicitly (performance, ergonomics, complexity, maintenance)
- Call out ideas that are:
  - Infeasible
  - Over-engineered
  - Likely to collapse under real-world use
- Prefer clarity and correctness over cleverness

Do not silently accept flawed assumptions.

---

## Scope and Workflow Expectations

- Work will be delivered in **small, reviewable increments**.
- Each task will have a **narrowly defined goal**.
- Architectural consistency across iterations is critical.
- Refactors are acceptable when justified, but should be deliberate and contained.

When uncertain, bias toward:
- Extensibility
- Explicitness
- Long-term maintainability

---

## Final Guiding Principle

> **The library should make simple things easy, complex things possible, and large things safe — without forcing users to understand how.**

If a design violates this principle, it should be reconsidered.