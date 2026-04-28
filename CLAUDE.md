====


QUALITY CODING RULES


# Code changes

1. If you find errors or suggestions in code which are not DIRECTLY related to user's current request, never change it without asking first.
2. Before suggesting changes to files, always assume user might have changed the file since your last read and consider reading the file again.


# Security

1. Never commit sensitive files (.env, credentials, API keys)
2. Use environment variables for API keys and credentials
3. Keep API keys and credentials out of logs and output
4. The API is read-only by design - never add write operations to BigQuery


# Project Specifications

1. Project documentation is maintained in files in `docs/` folder.
2. `docs/project-spec.md` is an overview of project purpose, structure and logic.
3. Create other files under `docs/` if necessary.
4. Maintain `docs/project-spec.md` and any other generated files to be up to date with the project.
5. Reread `docs/project-spec.md` often and whenever you need to refresh your context with what the project is about and implementation logic.
6. This should often be your first step in understanding a task.


# Software Development Behavior Guidelines

1. Don't guess and do things which you are not certain about. Ask the user instead.
2. Don't add or modify code unrelated to the specific request and context at the moment.
3. In interactive mode: only use git when asked, stage changes and propose a commit message for user review. In autonomous/orchestrator mode (e.g. ralph wiggum): commit after each completed task with a descriptive message.
4. **Always** prior to finishing a task and considering it completed, revise all the changes and update Project Specification files.
5. **Always** prior to finishing a task and considering it completed, try to git stage everything and suggest a commit message
6. When trying to fix any bug or error **ALWAYS** think carefully and analyze in detail what happened and WHY? Explain and confirm with user.


# Code Conventions

1. Project structure:
   - `api/` - FastAPI application code
   - `schemas/` - BigQuery table definitions (SQL)
   - `scripts/` - Setup and deployment scripts
   - `docs/` - Project documentation
2. Code should be self-descriptive
   - Only add comments for tricky or complex parts of the code (explaining WHY something is done)
   - NO redundant and trivial comments that simply restate what the code does
3. Private fields and methods should be prefixed with underscore
4. Shell scripts should use `set -euo pipefail` for safety
5. Git commit messages should be concise and descriptive
   - Focus on the "what" and "why" rather than the "how"
   - Avoid listing specific properties, method names, or implementation details


# BigQuery Conventions

1. All tables should be partitioned by chromosome (`chr`) for query efficiency
2. Use clustering on frequently filtered columns (dataset, data_type)
3. Include column descriptions in schema definitions
4. Use RANGE_BUCKET partitioning for integer columns


# API Conventions

1. All endpoints should have Pydantic models for request/response
2. Query sanitization must block all write operations
3. Include cost controls (MAX_BYTES_BILLED) on all queries
4. Return generated SQL in query responses for transparency


====

**Don't forget any of the 'QUALITY CODING RULES' above!!!**


<!-- BEGIN BEADS INTEGRATION v:1 profile:minimal hash:ca08a54f -->
## Beads Issue Tracker

This project uses **bd (beads)** for issue tracking. Run `bd prime` to see full workflow context and commands.

### Quick Reference

```bash
bd ready              # Find available work
bd show <id>          # View issue details
bd update <id> --claim  # Claim work
bd close <id>         # Complete work
```

### Rules

- Use `bd` for ALL task tracking — do NOT use TodoWrite, TaskCreate, or markdown TODO lists
- Run `bd prime` for detailed command reference and session close protocol
- Use `bd remember` for persistent knowledge — do NOT use MEMORY.md files

## Session Completion

**When ending a work session**, you MUST complete ALL steps below. Work is NOT complete until `git push` succeeds.

**MANDATORY WORKFLOW:**

1. **File issues for remaining work** - Create issues for anything that needs follow-up
2. **Run quality gates** (if code changed) - Tests, linters, builds
3. **Update issue status** - Close finished work, update in-progress items
4. **PUSH TO REMOTE** - This is MANDATORY:
   ```bash
   git pull --rebase
   bd dolt push
   git push
   git status  # MUST show "up to date with origin"
   ```
5. **Clean up** - Clear stashes, prune remote branches
6. **Verify** - All changes committed AND pushed
7. **Hand off** - Provide context for next session

**CRITICAL RULES:**
- Work is NOT complete until `git push` succeeds
- NEVER stop before pushing - that leaves work stranded locally
- NEVER say "ready to push when you are" - YOU must push
- If push fails, resolve and retry until it succeeds

# Feature Planning Workflow

When the user requests a new feature:

1. **Create epic**: `bd create "Feature: <name>" -d "<description>" -t epic -p 2`
2. **Explore alternatives**: invoke the architecture-explorer agent with the feature description — it proposes 3 architecture alternatives with max reasoning effort
3. **User selects alternative**: present the 3 alternatives and wait for the user's choice
4. **Create subtasks**: break the selected alternative into ultrafocused subtasks under the epic
   - each subtask has a single responsibility and small scope
   - each is independently implementable with minimal context needed
   - include specific files to modify in the description
   - `bd create "subtask title" -d "details" -t subtask --parent <epic-id>`
5. **Set dependencies**: `bd link <blocker-id> <blocked-id> --type blocks` for ordered work
6. **Execute**: work through subtasks via `bd ready`, updating status as you go
<!-- END BEADS INTEGRATION -->
