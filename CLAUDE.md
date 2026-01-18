# CLAUDE.md

Coding standards and best practices for all code in this repository.

## Core Principles

### Fail Loudly
Errors must never be silently swallowed. If something goes wrong, the system must make it obvious immediately.

- **Always throw or raise exceptions** when errors occur—never return error codes or null/None to indicate failure
- **Empty catch/except blocks are strictly forbidden**—every caught exception must be handled meaningfully or re-thrown
- **All errors must be logged with context**—include relevant state, parameters, and stack traces
- **Validate inputs at boundaries** and fail immediately with clear error messages if invalid

### Keep It Simple (KISS)
Write the simplest code that solves the problem. Complexity is a cost, not a feature.

- Prefer straightforward solutions over clever ones
- Avoid premature abstraction—wait until you have three concrete use cases
- Don't optimize until you've measured and identified a bottleneck

## Code Formatting

### Line Length
Maximum 120 characters per line.

### Auto-Formatters
All code **must** pass the project's configured formatter before commit:
- Python: Use Black, Ruff, or the configured formatter
- JavaScript/TypeScript: Use Prettier or the configured formatter
- Configure your editor to format on save

## Naming Conventions

### Follow Language Conventions
- Python: `snake_case` for functions and variables, `PascalCase` for classes
- JavaScript/TypeScript: `camelCase` for functions and variables, `PascalCase` for classes/components
- Constants: `SCREAMING_SNAKE_CASE` in all languages

### Be Descriptive
- Use full words, not abbreviations (`user_count` not `usr_cnt`, `calculateTotal` not `calcTot`)
- Names should reveal intent—a reader should understand purpose without checking the implementation
- Boolean variables should read as questions (`is_valid`, `hasPermission`, `shouldRetry`)

## Comments and Documentation

### Code Should Be Self-Documenting
Comments are a code smell—they often indicate the code isn't clear enough.

- **Don't comment WHAT** the code does—make the code readable enough that it's obvious
- **Only comment WHY** if the reason isn't apparent from context (e.g., workarounds, non-obvious business rules)
- If you feel the need to write a comment, first try to refactor the code to be clearer

### When Documentation Is Required
- Public API contracts (function signatures, module interfaces) should have type annotations that serve as documentation
- Complex algorithms may need a brief explanation of the approach
- Workarounds for bugs or edge cases should reference the issue/reason

## Type Annotations

Type annotations are **required** for all function signatures:

```python
def calculate_total(items: list[Item], tax_rate: float) -> Decimal:
    ...
```

```typescript
function calculateTotal(items: Item[], taxRate: number): number {
    ...
}
```

Benefits:
- Types serve as documentation
- Catch errors at compile/lint time rather than runtime
- Enable better IDE support and refactoring

## Error Handling

### Never Swallow Errors

```python
# WRONG - silent failure
try:
    process_data(data)
except Exception:
    pass

# WRONG - logging but continuing as if nothing happened
try:
    process_data(data)
except Exception as e:
    logger.error(f"Error: {e}")
    return None

# RIGHT - log with context and re-raise or handle meaningfully
try:
    process_data(data)
except DataProcessingError as e:
    logger.error(f"Failed to process data for user {user_id}: {e}", exc_info=True)
    raise
```

### Log With Context
When logging errors, include:
- What operation was being attempted
- Relevant identifiers (user ID, request ID, record ID)
- The full exception with stack trace (`exc_info=True` in Python, full error object in JS)

### Fail Fast
Validate inputs at the start of functions. Don't proceed with invalid state hoping it will work out.

```python
def process_order(order: Order) -> Receipt:
    if not order.items:
        raise ValueError("Cannot process order with no items")
    if order.total <= 0:
        raise ValueError(f"Invalid order total: {order.total}")
    # ... proceed with valid order
```

## Function Design

### Prefer Small, Focused Functions
- A function should do one thing well
- If you're struggling to name a function, it might be doing too many things
- No strict line limits, but if a function doesn't fit on one screen, consider splitting it

### Single Level of Abstraction
Functions should operate at a consistent level of abstraction. Don't mix high-level business logic with low-level implementation details in the same function.

## Testing

### Test-Driven Development (TDD)
Write tests before or alongside implementation code:

1. Write a failing test that defines the expected behavior
2. Write the minimum code to make the test pass
3. Refactor while keeping tests green

### Test Requirements
- All new functionality must have corresponding tests
- Bug fixes should include a regression test
- Tests should be deterministic and isolated

## Summary Checklist

Before committing code, verify:
- [ ] Code passes the auto-formatter
- [ ] All function signatures have type annotations
- [ ] No empty catch/except blocks
- [ ] All errors are logged with context before being raised/re-thrown
- [ ] Names are descriptive with no abbreviations
- [ ] Tests are written for new functionality
- [ ] Code is as simple as possible (no premature abstractions)
