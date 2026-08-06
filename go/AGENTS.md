# AGENTS.md

## General Rules

- Follow Effective Go.
- Follow standard Go naming conventions.
- Prefer the standard library over third-party packages.
- Keep exported APIs minimal.
- Preserve backward compatibility unless explicitly instructed otherwise.
- Never modify public APIs without approval.

---

## Code Guidelines

### File Header

Every Go source file must begin with the following header.

```go
//
// FILE_NAME.go
//
```

Example:

```amount.go
//
// amount.go
//
```

Do not include copyright notices unless requested.

### Function Comments

Every exported function, method must include a GoDoc comment.
GoDoc comments are not required for exported types, interfaces, variables, or constants unless explicitly requested.

Use the following template.

```go
//
// VERB XXX.
//
// Parameters:
//   - XXX
//
// Returns:
//   - XXX
//
// Examples:
//   - XXX
//
// Version:
//   - 2026-01-01: Added.
//
```

Rules:

- Begin with the identifier name.
- Omit unnecessary sections.
- Keep comments concise.
- Use imperative verbs when appropriate.
- Update the Version section whenever behavior changes.

Example:

```go
//
// ParseAmount parses a decimal amount.
//
// Parameters:
//   - value: decimal string
//
// Returns:
//   - Parsed amount.
//
// Version:
//   - 2026-08-06: Added.
//
func ParseAmount(value string) (Amount, error)
```

### Error Handling

- Return errors instead of panicking.
- Wrap errors using fmt.Errorf("%w", err).
- Include useful contextual information.
- Never ignore returned errors.
- Avoid logging inside reusable libraries unless explicitly requested.





