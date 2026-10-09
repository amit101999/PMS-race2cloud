# CLAUDE.md

# Coding Philosophy

Write code that is:

- Correct
- Simple
- Readable
- Maintainable
- Explainable
- Production-quality

The primary goal is NOT to create the most sophisticated architecture.

The primary goal is:

> **Use the simplest production-quality solution that correctly solves the actual problem.**

Do not optimize code for impressiveness, abstraction, or theoretical scalability.

Optimize for **clarity and correctness first**.

---

# 1. Core Principle: Simple Does Not Mean Poor Quality

"Simple" means:

- Easy to read
- Easy to debug
- Easy to explain
- Easy to modify
- Uses appropriate abstractions
- Handles important errors and edge cases
- Follows security best practices
- Uses the existing project architecture appropriately

"Simple" does NOT mean:

- Ignoring errors
- Removing validation
- Ignoring security
- Ignoring important edge cases
- Hardcoding sensitive information
- Avoiding tests
- Using poor architecture
- Sacrificing correctness
- Ignoring performance when performance matters

The target is:

**SIMPLE + CORRECT + MAINTAINABLE**

Not:

**SIMPLE + FRAGILE**

---

# 2. Avoid Over-Engineering

Do not introduce complexity unless there is a concrete reason.

Before adding an abstraction, pattern, class, service, dependency, or architectural layer, ask:

> "What real problem does this solve right now?"

If there is no clear answer, do not introduce it.

Do not design for hypothetical future requirements.

Avoid building infrastructure for features that do not exist yet.

Do not add flexibility merely because something "might be needed later."

---

# 3. Prefer Straightforward Implementations

When multiple approaches are correct, prefer the one with:

1. Fewer concepts
2. Fewer files
3. Fewer abstractions
4. Less indirection
5. Fewer dependencies
6. Easier debugging
7. Easier explanation

provided correctness, security, maintainability, and required performance remain intact.

Prefer obvious code over clever code.

For example, prefer:

```text
fetch data
validate data
process data
return result
```

over creating multiple layers that obscure this simple flow.

---

# 4. Do Not Create Unnecessary Abstractions

Avoid creating abstractions such as:

- Interfaces used only once
- Classes used only once
- Managers
- Handlers
- Factories
- Builders
- Repositories
- Adapters
- Wrappers
- Generic utility layers
- Custom frameworks
- Custom dependency injection systems

unless they provide a genuine benefit.

Do not create an abstraction simply because a design pattern exists for the problem.

Design patterns are tools, not requirements.

Use them only when they make the code genuinely better.

---

# 5. Avoid Excessive Layering

Do not automatically create architectures such as:

```text
Controller
    ↓
Service
    ↓
Manager
    ↓
Handler
    ↓
Factory
    ↓
Repository
    ↓
Adapter
    ↓
Utility
```

if the problem does not require this level of separation.

Use layers when they provide meaningful separation of responsibilities.

A small feature may only need:

```text
Route → Function → Database
```

A larger feature may reasonably need:

```text
Controller → Service → Repository
```

Choose the architecture based on actual complexity.

Do not force enterprise architecture onto a small application.

---

# 6. Respect the Existing Codebase

Before writing code:

1. Inspect the existing project structure.
2. Understand how similar features are implemented.
3. Identify existing utilities and services.
4. Follow existing conventions where they are reasonable.
5. Reuse existing dependencies and patterns when appropriate.

Do not introduce a completely new architectural style for one feature.

Prefer consistency with the existing codebase.

However, do not blindly copy bad patterns.

If the existing implementation has an obvious problem, improve it only when it is relevant to the task.

---

# 7. Keep Changes Focused

Only modify what is necessary for the requested task.

Do NOT:

- Refactor unrelated files
- Rename unrelated variables
- Reorganize the entire project
- Replace working libraries without a reason
- Rewrite existing components unnecessarily
- Change architecture without necessity
- Add unrelated features
- "Clean up" unrelated code

A focused change is preferred over a large rewrite.

If a broader refactor is genuinely necessary, explain why before doing it.

---

# 8. Before Creating a New File

Before creating a new file, ask:

> "Does this functionality genuinely deserve its own module?"

If the code is small, local, and used only once, keeping it in the existing file may be better.

Do not create files simply to make the directory structure look "clean."

Good organization is useful.

Fragmenting simple logic across many files is not.

---

# 9. Functions

Prefer small, focused functions.

A function should generally have one clear responsibility.

However:

**Do not split every few lines into a separate function.**

Avoid this:

```javascript
const getUserId = () => user.id;
const validateUserId = (id) => !!id;
const prepareUserId = (id) => String(id);
```

when the logic is trivial and only used once.

Prefer readable direct code.

Extract a function when:

- It is reused
- It represents a meaningful operation
- It improves readability
- It isolates genuinely complex logic
- It makes testing easier

---

# 10. Variable and Function Names

Prefer descriptive names.

Bad:

```javascript
const d = getData();
const x = d.items;
const r = process(x);
```

Better:

```javascript
const response = getData();
const items = response.items;
const processedItems = process(items);
```

Do not use unnecessarily verbose names either.

Names should communicate intent without requiring comments.

---

# 11. Comments

Do not comment obvious code.

Bad:

```javascript
// Increment count
count++;
```

Good:

```javascript
// The API can return duplicate events, so ignore events
// that have already been processed.
```

Comments should explain:

- Why something is done
- Important business rules
- Non-obvious constraints
- Workarounds
- External API behavior
- Important security considerations

Comments should NOT simply describe what the code obviously does.

---

# 12. Error Handling

Handle errors appropriately.

Do not ignore errors.

Do not blindly wrap every function in try/catch.

Use error handling where it provides value.

Avoid:

```javascript
try {
  // almost everything
} catch (error) {
  console.log(error);
}
```

Prefer meaningful handling:

```javascript
try {
  const user = await getUser(userId);
  return user;
} catch (error) {
  logger.error("Failed to fetch user", error);
  throw error;
}
```

Do not hide failures merely to make the application appear successful.

---

# 13. Validation

Validate external input where appropriate.

Treat data coming from:

- Users
- APIs
- HTTP requests
- Forms
- Query parameters
- Environment variables
- External services

as potentially invalid.

However, do not create excessive validation layers for trusted internal data.

Validate at meaningful boundaries.

---

# 14. Security

Security is more important than simplicity.

Never:

- Hardcode passwords
- Hardcode API keys
- Commit secrets
- Expose sensitive information
- Trust user input blindly
- Disable authentication without a reason
- Disable authorization checks
- Store credentials insecurely

Use established security mechanisms provided by the framework or existing dependencies.

Do not implement custom cryptography or authentication when established solutions exist.

---

# 15. Dependencies

Before adding a dependency, ask:

> "Can this reasonably be solved with the language, framework, or dependencies already in the project?"

Do not add a package for trivial functionality.

For example, do not install a package solely to perform a simple transformation that can be clearly written using existing language features.

When a dependency genuinely provides significant value, use it.

Do not reinvent complex functionality unnecessarily.

---

# 16. Do Not Optimize for Hypothetical Scale

Do not introduce distributed systems, caching layers, queues, microservices, event buses, complex state management, or advanced database architecture unless the requirements justify them.

Do not say:

> "This will be more scalable in the future."

unless there is evidence that the current requirements require it.

Build for the actual requirements.

If the application later grows, architecture can evolve.

---

# 17. Performance

Do not prematurely optimize.

First prioritize:

1. Correctness
2. Readability
3. Maintainability
4. Performance

unless performance is explicitly a critical requirement.

However, avoid obviously inefficient solutions when a similarly simple efficient solution exists.

Prefer:

```text
Simple + Efficient
```

over:

```text
Complex + Slightly More Efficient
```

unless the performance difference is meaningful.

---

# 18. TypeScript Rules

Prefer simple, useful types.

Avoid excessive type-level programming.

Do not create complicated generic types merely to demonstrate type safety.

Avoid unnecessary:

- Generic abstractions
- Conditional types
- Mapped types
- Utility-type chains
- Type gymnastics

when a straightforward interface/type is sufficient.

Prefer:

```typescript
interface User {
  id: string;
  name: string;
  email: string;
}
```

over unnecessarily complex type constructions.

Type safety matters.

Complexity for its own sake does not.

---

# 19. React Rules

For React applications:

Prefer:

- Simple components
- Local state when possible
- Clear props
- Straightforward hooks
- Reusable components only when reuse is meaningful

Avoid:

- Excessive custom hooks
- Global state for local state
- Complex state-management libraries for simple state
- Excessive component splitting
- Abstraction layers around simple components

Do not create a custom hook simply because logic technically could be placed in one.

Extract hooks when they improve reuse or readability.

---

# 20. Backend Rules

For backend applications:

Prefer clear request → validation → business logic → response flow.

Do not create unnecessary service/repository/controller layers for tiny features.

For larger systems, use proper separation where it provides real benefits.

Handle:

- Authentication
- Authorization
- Validation
- Errors
- Logging
- Database failures
- External API failures

appropriately.

Do not sacrifice backend correctness for the sake of fewer lines of code.

---

# 21. AI / LLM Application Rules

When building AI applications:

Prefer simple and understandable flows.

Avoid unnecessarily complex agent architectures.

Do not introduce:

- Multiple agents
- Complex orchestration
- Excessive tool layers
- Unnecessary memory systems
- Complex state machines
- Multiple abstraction layers

unless the actual problem requires them.

Prefer:

```text
User
 ↓
LLM
 ↓
Tool
 ↓
Result
```

when that is sufficient.

Use more complex agent architectures only when they provide a real benefit.

For AI-generated output, prioritize:

- Validation
- Guardrails
- Error handling
- Clear prompts
- Structured outputs
- Observability

when required by the application.

---

# 22. Do Not Overuse Design Patterns

Do not use a design pattern simply because it is available.

Before using a pattern, ask:

1. What problem does it solve?
2. Does the current code actually have that problem?
3. Does the pattern make the code easier to understand?
4. Would a simpler implementation work?

If the simpler implementation works, use it.

---

# 23. No "Enterprise" Code for Small Problems

Do not turn a simple feature into an enterprise architecture.

For example, if the requirement is:

> "Create an API endpoint that returns users."

Do not automatically create:

```text
UserController
UserService
UserServiceInterface
UserRepository
UserRepositoryInterface
UserMapper
UserDTO
UserFactory
UserValidator
UserResponseBuilder
```

unless the project genuinely requires them.

A simple application may only need:

```text
GET /users
    ↓
query database
    ↓
return users
```

Architecture should match complexity.

---

# 24. Avoid Clever Code

Do not write code that is technically impressive but difficult to understand.

Avoid unnecessary:

- One-liners
- Nested ternaries
- Clever array transformations
- Metaprogramming
- Magic behavior
- Implicit side effects
- Extremely compact expressions

when straightforward code is clearer.

Readable code is more valuable than clever code.

---

# 25. Prefer Explicit Code When It Improves Understanding

If an explicit implementation is easier to understand, use it.

For example, prefer:

```javascript
if (!user) {
  return null;
}

if (!user.isActive) {
  return null;
}

return user;
```

over compressing everything into a clever expression if the latter reduces readability.

Code should communicate intent.

---

# 26. Don't Repeat Yourself — But Don't Over-Abstract Either

DRY is useful, but excessive DRY can make code harder to understand.

Do not immediately abstract two similar pieces of code.

Duplication can be acceptable when:

- The logic is simple
- The code is unlikely to change together
- Combining it would require a complicated abstraction

Prefer:

**A little duplication**

over:

**A confusing generic abstraction**

when appropriate.

---

# 27. Testing

Write tests when they provide meaningful confidence.

Prioritize testing:

- Business logic
- Important edge cases
- Authentication/authorization
- Critical API behavior
- Data transformations
- Important AI behavior
- Regression-prone functionality

Do not create meaningless tests simply to increase test count.

Tests should be understandable too.

---

# 28. Logging

Use logging appropriately.

Do not log:

- Passwords
- API keys
- Tokens
- Sensitive user information
- Secrets

Do not add excessive logging everywhere.

Log information that helps diagnose meaningful failures.

---

# 29. Configuration

Do not create complicated configuration systems for simple applications.

Use environment variables and existing project configuration mechanisms appropriately.

Avoid creating custom configuration abstractions unless necessary.

---

# 30. Reuse Existing Code

Before creating something new, search the codebase.

Check whether:

- A utility already exists
- A component already exists
- A service already exists
- A validation function already exists
- A type already exists
- A dependency already provides the functionality

Reuse existing functionality when it is appropriate.

Do not duplicate functionality unnecessarily.

---

# 31. Explainability Requirement

The code should be understandable enough that I can explain it during:

- A technical interview
- A code review
- A project demonstration
- Debugging
- A discussion with another developer

For important implementations, I should be able to answer:

- What does this code do?
- Why is it structured this way?
- Where does the data come from?
- Where does the data go?
- What happens when something fails?
- Why was this approach chosen?

If understanding the implementation requires understanding several unnecessary abstractions, simplify it.

---

# 32. Complexity Budget

Every implementation has a complexity budget.

Spend complexity only when it provides real value.

Before introducing:

- A new abstraction
- A new library
- A new architectural layer
- A new design pattern
- A new state-management system
- A new service
- A new file

ask whether the benefit justifies the complexity.

If two solutions are equally correct, choose the simpler one.

---

# 33. No Impressiveness Optimization

Do NOT make code more sophisticated simply to demonstrate advanced programming ability.

Do not use:

- Advanced patterns unnecessarily
- Complex generics unnecessarily
- Type-level programming unnecessarily
- Functional programming techniques unnecessarily
- Metaprogramming unnecessarily
- Complex state machines unnecessarily
- Advanced architecture unnecessarily

Code quality is NOT measured by how sophisticated the implementation looks.

Code quality is measured by:

**Correctness + Clarity + Maintainability + Appropriate Architecture**

---

# 34. Implementation Process

When given a coding task:

## Step 1 — Understand

Understand:

- What is being requested
- Existing architecture
- Existing conventions
- Relevant files
- Dependencies
- Constraints

Do not start changing code immediately.

## Step 2 — Plan

Create the smallest reasonable implementation plan.

Avoid designing the entire system if only a small feature is required.

## Step 3 — Implement

Implement the solution using existing project patterns where appropriate.

Keep the change focused.

## Step 4 — Verify

Check:

- Correctness
- Error handling
- Validation
- Security
- Edge cases
- Type errors
- Lint errors
- Tests

Use the project's existing verification commands.

## Step 5 — Simplify

Before finishing, review the implementation specifically for unnecessary complexity.

Ask:

> "Can this be simpler without losing correctness or maintainability?"

If yes, simplify it.

---

# 35. Mandatory Final Self-Review

Before declaring the task complete, perform this review:

### Complexity

- Did I introduce unnecessary abstractions?
- Did I create unnecessary files?
- Did I add unnecessary dependencies?
- Did I introduce unnecessary architecture?
- Did I overuse design patterns?
- Did I optimize for hypothetical future requirements?

### Readability

- Can another developer understand this quickly?
- Are names clear?
- Is the control flow obvious?
- Is the data flow easy to follow?
- Are functions appropriately sized?
- Did I make anything clever unnecessarily?

### Correctness

- Does the implementation satisfy the actual requirement?
- Are important edge cases handled?
- Is error handling appropriate?
- Is validation present where needed?
- Did I preserve existing behavior?

### Security

- Are secrets protected?
- Is user input handled safely?
- Are authentication and authorization preserved?
- Did I accidentally expose sensitive information?

### Scope

- Did I modify unrelated code?
- Did I perform unnecessary refactoring?
- Did I add features that were not requested?

If unnecessary complexity is found, simplify the implementation before finishing.

---

# 36. Priority Order

When making implementation decisions, follow this priority:

1. **Correctness**
2. **Security**
3. **Requirements**
4. **Maintainability**
5. **Readability**
6. **Appropriate architecture**
7. **Performance**
8. **Extensibility**

Do not sacrifice correctness or security for simplicity.

Do not sacrifice readability for theoretical extensibility.

---

# 37. Default Decision Rule

When uncertain between two valid implementations:

> **Choose the simplest implementation that satisfies the requirements without creating meaningful technical debt.**

Do not choose the solution that looks more impressive.

Do not choose the solution with more abstractions.

Do not choose the solution that is more "enterprise" by default.

Choose the solution that a competent developer can read, understand, debug, explain, and maintain.

---

# Final Principle

Always remember:

> **Make the code as simple as possible, but no simpler.**

The objective is not to write less code.

The objective is to write **less unnecessary complexity**.

Write code that is:

**Simple enough to understand.
Robust enough to trust.
Flexible enough for real requirements.
Straightforward enough to explain.**

---

# Business Documentation (docs/PMS documentation (business flow).docx)

The business guide explains the system to the operations team and new teammates.

Its current style and structure are the standard. Preserve them.

## When to Update

- When a change alters behaviour described in the guide (rules, inputs, formulas, dates, screens, reports), update the guide in the same task and list the sections changed in the final report.
- Do not document behaviour that is not built yet as if it works. Use "Planned but not built yet" / "not available yet", and remove that wording once it is built.
- Update the Subtitle date when the guide changes.

## What to Write About

- Describe what the system does for the business, from the operator's point of view.
- Do not include code, table or column names, API routes, file paths, Catalyst/ZCQL details or internal workarounds. Those belong in code comments.

## Writing Style

- Simple English, short sentences, present tense, one idea per sentence.
- Explain any technical term in brackets the first time it is used.
- After an important rule, give the reason in one short sentence.
- Write formulas in words with the result in bold, for example: "**New holding** = old holding × Y ÷ X". Use ×, ÷, −, ₹.
- Give a small worked example with real numbers for any calculation.
- Use the guide's existing terms: the operator, our books / FA, the custodian, actual code, virtual code, record date, effective date.
- Name buttons exactly as they appear on screen, in bold.

## Structure

- Keep the order: 1 Introduction, 2 Business flow, 3 Modules, 4 Key business rules at a glance, 5 Glossary.
- Modules (3.x) go in the order the work happens. A new screen gets a new 3.x section in its workflow position. Renumber later sections and fix every "(3.x)" reference.
- Each module section: an opening definition paragraph, then bullets. Group longer sections under bold lead-in sentences ending in a full stop (for example "**Which date counts.**").
- Corporate action sections follow: definition, **Inputs:**, formula, example, rounding, cost, edge cases, planned work.
- Short steps: numbered list. Steps that need their own bullets: bold "**Step N – Name.**" paragraphs.
- A new rule that matters for checking correctness also gets a row in Section 4. A new term also gets a Glossary row, in alphabetical order.
- If the daily cycle or the corporate action cycle changes, update Section 2 and its diagram.

## Formatting

- Use the existing Word styles only: Title, Subtitle, Heading 1 ("N. Title"), Heading 2 ("N.M Name"), First Paragraph, Body Text, Compact (list items), Image Caption.
- Use tables only for lists of named things (types, files, reports, rules, terms): 2–3 columns with a header row.
- Use an en dash ( – ) after bold labels, and curly quotes.
- Do not restyle, reorder or rewrite sections unrelated to the change.

---

# Developer Module Map (docs/developer-module-map.md)

- When a change adds, removes, renames or newly shares a file, background function or database table, update the module map in the same task.
- When editing a file listed as shared or copied, check the modules listed against it, and update every copy of copied code together (or say why a copy differs).
- Keep the existing layout: shared index tables first, then one section per module using the same template.
