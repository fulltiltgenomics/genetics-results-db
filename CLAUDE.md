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
3. When using git, only stage changes and propose a commit message. Let the user review the changes and commit them.
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
