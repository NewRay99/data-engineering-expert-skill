# Maintenance & Extension Guide

This guide covers how to maintain, extend, and back up the `data-engineering-expert` skill.

## Skill Structure

```
data-engineering-expert/
├── SKILL.md              # Main entry point — do not exceed 100k chars
├── references/           # 13 detailed guides (3k–30k bytes each)
├── templates/            # 7 copy-and-modify starter files
└── scripts/              # 2 runnable Python/bash scripts
```

## Adding New Reference Docs

When a new Databricks or Azure feature is adopted (after system testing per the evaluation process in SKILL.md):

1. Create the reference file:
   ```
   skill_manage(action='write_file', name='data-engineering-expert',
     file_path='references/new-feature-name.md', file_content='...')
   ```
2. Add a one-line pointer in SKILL.md under the relevant section:
   ```
   **Detailed guide:** `references/new-feature-name.md`
   ```
3. Commit and push to GitHub (see below).

## Adding New Templates or Scripts

- Templates go under `templates/` — these are starter files meant to be copied and modified.
- Scripts go under `scripts/` — these are statically re-runnable (verification, fixtures, probes).
- Add a pointer in SKILL.md so future agents discover them.

## Bulk Additions — Parallel Subagent Delegation

When adding multiple reference docs, templates, or scripts at once, use `delegate_task` with batch mode (up to 3 parallel subagents). This technique was used to create the initial 23 files in this skill:

1. Draft the main SKILL.md first — it defines the structure and references files that don't exist yet.
2. Spawn 3 parallel leaf subagents, each responsible for a set of files:
   - Subagent 1: Databricks-related references (medallion, ADF, transformation, DLT)
   - Subagent 2: Data pattern references (DQ, MDM, time-series, metadata)
   - Subagent 3: Engineering practice references (testing, git, pre-commit, onboarding)
3. Give each subagent the exact file paths and content requirements.
4. Verify all files exist with `find` or `search_files`.
5. Verify the skill loads with `skill_view(name='data-engineering-expert')`.

**Key details:**
- Subagents get isolated contexts — pass all file paths and content specs via `context` or `goal`.
- Subagents can use `write_file` (from the `file` toolset).
- Subagents inherit the configured model (e.g., GLM 5.2) even if the parent session is on a different model.
- Each subagent should write 2–4 files to stay within output token limits.

## GitHub Backup

This skill is backed up to GitHub at:
`https://github.com/NewRay99/data-engineering-expert-skill`

### Pushing Updates

```bash
cd "$HOME/AppData/Local/hermes/skills/data-engineering/data-engineering-expert"
git add -A
git commit -m "docs: update <what changed>"
git push origin main
```

### Cloning to a New Machine

```bash
git clone https://github.com/NewRay99/data-engineering-expert-skill.git \
  "$HOME/AppData/Local/hermes/skills/data-engineering/data-engineering-expert"
```

## Monthly Review Checklist

Per the `references/official-documentation-links.md` guide:

- [ ] Check Databricks release notes for new features and patches
- [ ] Check Azure updates for ADF and related services
- [ ] Evaluate any new features using the 7-step process in SKILL.md
- [ ] Update reference docs if features are adopted
- [ ] Commit and push changes to GitHub
- [ ] Run `skill_view(name='data-engineering-expert')` to verify skill integrity

## Common Maintenance Pitfalls

1. **Exceeding SKILL.md size limit.** The main file must stay under 100k chars. If growing, move detail to `references/`.

2. **Forgetting to add pointers.** New reference files are invisible to the skill loader unless SKILL.md mentions them. Always add a `**Detailed guide:** references/xxx.md` line.

3. **Not pushing to GitHub.** Local-only changes are fragile. Always commit and push after updates.

4. **Stale links.** Databricks and Azure docs URLs change. Validate links in `references/official-documentation-links.md` quarterly.

5. **Inconsistent naming.** Reference files use kebab-case. Templates use kebab-case with appropriate extension. Scripts use snake_case for Python, kebab-case for shell.
