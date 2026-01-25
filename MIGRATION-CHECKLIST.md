# ContextBrain Migration Checklist

## ✅ Completed

- [x] Оновлено `contextbrain-fulldoc.md` з інформацією з `brain-fulldoc.md`
- [x] Видалено `brain-fulldoc.md`
- [x] Виправлено посилання в `.github/workflows/publish.yml` (contextrouter → contextbrain)
- [x] Виправлено посилання в `.cursorrules` (ContextRouter → ContextBrain)
- [x] Виправлено посилання в `.bandit` (ContextRouter → ContextBrain)
- [x] Виправлено посилання в `CONTRIBUTING.md` (ContextRouter → ContextBrain)
- [x] Оновлено `README.md` з посиланнями на ContextBrain
- [x] Додано конфігурацію pytest і coverage в `pyproject.toml`
- [x] Видалено старий `coverage.xml` з неправильним шляхом
- [x] Перейменовано `ContextrouterError` → `ContextbrainError` в `core/exceptions.py`
- [x] Виправлено всі імпорти з `contextrouter.*` на `contextbrain.*` в storage модулях
- [x] Виправлено тести (`test_exceptions_contract.py`)
- [x] Виправлено приклади (`05_custom_errors.py`)
- [x] Оновлено user agent strings (ContextrouterIngestionBot → ContextbrainIngestionBot)
- [x] Оновлено docstrings та коментарі в config модулях

## ⚠️ Requires Manual Review

### Import Statements in Code

**Found imports that still reference `contextrouter`:**

1. **Storage modules:**
   - `src/contextbrain/storage/vertex_grounding.py` - imports from `contextrouter.core`
   - `src/contextbrain/storage/vertex.py` - imports from `contextrouter.core`
   - `src/contextbrain/storage/vertex_search.py` - imports from `contextrouter.modules`
   - `src/contextbrain/storage/postgres/provider.py` - imports from `contextrouter.core` and `contextrouter.modules`
   - `src/contextbrain/storage/gcs.py` - imports from `contextrouter.core`

2. **Examples:**
   - `examples/ner_usage.py` - mentions ContextRouter
   - `examples/keyphrase_usage.py` - mentions ContextRouter
   - `examples/04_custom_graph_tracing.py` - mentions ContextRouter
   - `examples/custom_graph.py` - mentions ContextRouter
   - `examples/05_custom_errors.py` - imports from `contextbrain.core.exceptions` (correct)

3. **Core modules:**
   - `src/contextbrain/core/config/main.py` - has `CONTEXTROUTER_*` env var names (may be intentional for backward compatibility)
   - `src/contextbrain/core/config/__init__.py` - mentions ContextRouter
   - `src/contextbrain/core/config/security.py` - mentions ContextRouter
   - `src/contextbrain/core/registry.py` - mentions ContextRouter
   - `src/contextbrain/core/exceptions.py` - defines `ContextrouterError` (should be `ContextbrainError`)

4. **Ingestion:**
   - `src/contextbrain/ingestion/rag/config.py` - has `CONTEXTROUTER_*` env var names
   - `src/contextbrain/ingestion/rag/settings.py` - mentions "ContextrouterIngestionBot"
   - `src/contextbrain/ingestion/rag/upload_providers/gcloud.py` - has `CONTEXTROUTER_*` env var names
   - `src/contextbrain/ingestion/rag/plugins/web.py` - mentions "ContextrouterIngestionBot"

### Test Files

- `tests/unit/test_exceptions_contract.py` - checks for `ContextrouterError` (should check for `ContextbrainError`)

### Configuration Files

- `pyproject.toml` - Repository URL still points to `ContextRouter/contextbrain` (may be intentional if org name is ContextRouter)

## 🔧 Action Items

### High Priority (Breaking Changes)

1. **Rename Exception Class:** ✅ DONE
   - `ContextrouterError` → `ContextbrainError` in `src/contextbrain/core/exceptions.py`
   - Updated all imports and references
   - Updated test files

2. **Fix Import Statements:** ✅ DONE
   - All `from contextrouter.*` → `from contextbrain.*` in storage providers
   - Fixed all storage modules (vertex, postgres, gcs)
   - Fixed examples and tests

### Medium Priority (Non-Breaking)

3. **Environment Variables:** ✅ DONE
   - All `CONTEXTROUTER_*` env vars replaced with `CONTEXTBRAIN_*`
   - Updated in: core/config/main.py, ingestion/rag/config.py, upload_providers/gcloud.py
   - Updated examples/env.example and documentation

4. **User-Agent Strings:** ✅ DONE
   - "ContextrouterIngestionBot" → "ContextbrainIngestionBot"

5. **Comments and Documentation:** ⚠️ PARTIAL
   - Updated docstrings in core modules
   - Examples still have ContextRouter mentions (non-critical, can update later)

### Low Priority (Documentation)

6. **Examples:**
   - Update example code comments
   - Update README in examples directory

7. **Test Coverage:**
   - Run tests to verify they still work after migration
   - Check test coverage report

## 📊 Test Status

**To verify tests work:**
```bash
cd contextbrain
uv sync --dev
uv run pytest -v
uv run pytest --cov=src/contextbrain --cov-report=html
```

## 🔍 Verification Steps

1. [ ] Run all tests: `pytest -v`
2. [ ] Check test coverage: `pytest --cov`
3. [ ] Verify imports work: `python -c "import contextbrain"`
4. [ ] Check for linting errors: `ruff check .`
5. [ ] Verify type checking: `mypy src/contextbrain` (if configured)

## 📝 Notes

- Some references to `contextrouter` in env vars may be intentional for backward compatibility
- Repository URL in pyproject.toml may stay as `ContextRouter/contextbrain` if the GitHub org is still named ContextRouter
- Exception class rename is a breaking change - consider version bump