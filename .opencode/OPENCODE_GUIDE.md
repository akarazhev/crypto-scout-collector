# OpenCode Agent Guide for crypto-scout-collector

This guide explains how to use the configured OpenCode agents and skills for developing, reviewing, and documenting the crypto-scout-collector microservice.

## Quick Start

```bash
# Install OpenCode (if not already installed)
# See: https://opencode.ai/docs/installation

# Navigate to project
cd /path/to/crypto-scout-collector

# Start OpenCode
opencode
```

## Available Agents

### 1. Developer Agent (`developer`)

**Purpose**: Primary agent for implementing features, fixing bugs, and maintaining the microservice.

**Mode**: Primary (use Tab to switch to this agent)

**Capabilities**:
- Full read/write access to files
- Can execute bash commands (build, test)
- Access to all project skills

**When to Use**:
- Implementing new test utilities
- Adding mock data fixtures
- Fixing bugs
- Refactoring code
- Running builds and tests

**Example Prompts**:
```
Add a new MockData.Type for WebSocket ticker data

Implement a new utility method in DBUtils to truncate tables

Fix the timeout issue in PodmanCompose.waitForDatabaseReady()

Add support for Bybit inverse contracts in MockData
```

### 2. Code Reviewer Agent (`reviewer`)

**Purpose**: Reviews code for quality, security, and adherence to project conventions.

**Mode**: Subagent (invoke with `@reviewer`)

**Capabilities**:
- Read-only access to files
- Can search and grep codebase
- Access to code style skills

**When to Use**:
- Before merging changes
- Checking code quality
- Verifying style compliance
- Security audits

**Example Prompts**:
```
@reviewer Review the changes in StreamTestPublisher.java

@reviewer Check if MockData.java follows project conventions

@reviewer Audit the error handling in PodmanCompose

@reviewer Review the new test class for best practices
```

### 3. Technical Writer Agent (`writer`)

**Purpose**: Creates and maintains documentation for the library.

**Mode**: Subagent (invoke with `@writer`)

**Capabilities**:
- Read/write access to documentation files
- Can read source code for reference
- Access to documentation skills

**When to Use**:
- Updating README.md
- Writing API documentation
- Creating usage examples
- Maintaining AGENTS.md

**Example Prompts**:
```
@writer Update README.md with the new MockData types

@writer Add Javadoc to the DBUtils class

@writer Create a troubleshooting section for RabbitMQ issues

@writer Document the new configuration properties
```

## Available Skills

Skills are loaded on-demand by agents. View available skills in your session.

### java-microservice
Microservice development patterns including modules, AMQP publishing, and WebSocket consumers.

### java-code-style
Java 25 code style conventions for naming, imports, error handling, and testing patterns.

### podman-deployment
Podman Compose deployment patterns for containerization and RabbitMQ integration.

## Usage Patterns

### Feature Development Workflow

1. **Start with the developer agent** (Tab to switch if needed)
2. Describe the feature you want to implement
3. The agent will use skills to understand project conventions
4. Review generated code
5. **Invoke reviewer**: `@reviewer Check the new implementation`
6. Address feedback
7. **Invoke writer**: `@writer Update documentation for the new feature`

### Code Review Workflow

```
@reviewer Review src/main/java/com/github/akarazhev/cryptoscout/module/CoreModule.java

@reviewer Check all files changed in the last commit for style violations

@reviewer Audit the error handling across the entire codebase
```

### Documentation Workflow

```
@writer Update the README with current features

@writer Add examples for using StreamTestPublisher

@writer Create a configuration reference table
```

## Agent Navigation

| Action | Keybind |
|--------|---------|
| Switch primary agent | `Tab` |
| Invoke subagent | `@agentname message` |
| Cycle child sessions | `<Leader>+Right` |
| Cycle back | `<Leader>+Left` |

## Configuration

Agents are configured in `.opencode/agents/`. To customize:

### Modify Agent Behavior
Edit the agent's markdown file (e.g., `.opencode/agents/developer.md`):
- Adjust `temperature` for creativity vs. determinism
- Enable/disable specific `tools`
- Modify the system prompt

### Add New Skills
Create a new skill in `.opencode/skills/<skill-name>/SKILL.md`:
```yaml
---
name: skill-name
description: Brief description for agent discovery
license: MIT
compatibility: opencode
---

## What I Do
...

## When to Use Me
...
```

### Override Model
Add `model` to agent frontmatter:
```yaml
model: zai-coding-plan/glm-4.7
```

## Best Practices

### For Development
1. Always run tests after changes: `mvn test`
2. Use `@reviewer` before finalizing changes
3. Keep changes focused and incremental
4. Follow the code style skill guidelines

### For Reviews
1. Be specific about what to review
2. Ask for specific aspects (security, performance, style)
3. Request actionable feedback

### For Documentation
1. Keep examples working and tested
2. Update docs alongside code changes
3. Use consistent formatting

## Troubleshooting

### Agent Not Found
Verify agent files exist in `.opencode/agents/`:
```bash
ls -la .opencode/agents/
```

### Skill Not Loading
1. Check SKILL.md is uppercase
2. Verify frontmatter has `name` and `description`
3. Ensure skill name matches directory name

### Permission Issues
Check agent's `tools` configuration in frontmatter:
```yaml
tools:
  write: true
  edit: true
  bash: true
```

## File Structure

```
.opencode/
├── agents/
│   ├── developer.md    # Primary development agent
│   ├── reviewer.md     # Code review subagent
│   └── writer.md       # Documentation subagent
├── skills/
│   ├── java-microservice/
│   │   └── SKILL.md    # Microservice development patterns
│   ├── java-code-style/
│   │   └── SKILL.md    # Code style conventions
│   └── podman-deployment/
│       └── SKILL.md    # Container deployment patterns
└── OPENCODE_GUIDE.md   # This guide
```

## Additional Resources

- [OpenCode Documentation](https://opencode.ai/docs/)
- [Agent Configuration](https://opencode.ai/docs/agents/)
- [Skills Reference](https://opencode.ai/docs/skills)
- [Project README](../README.md)
- [Project AGENTS.md](../AGENTS.md)
