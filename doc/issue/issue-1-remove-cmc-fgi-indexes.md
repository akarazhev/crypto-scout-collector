# Issue 1: Removing of the `idx_cmc_fgi_score` and `idx_cmc_fgi_name` indexes

In this `crypto-scout-collector-db` project we are going to remove the `idx_cmc_fgi_score` and `idx_cmc_fgi_name` indexes.

## Roles

Take the following roles:

- Expert database engineer.

## Conditions

- Use the best practices and design patterns.
- Do not hallucinate.

## Tasks

- As the expert database engineer review the current `init.sql` script implementation in `crypto-scout-collector-db` project and
  update it by removing the `idx_cmc_fgi_score` and `idx_cmc_fgi_name` indexes.
- Update the documentation `crypto-scout-collector-db-issues-ai.md` with your proposal.
- Recheck your proposal and make sure that they are correct and haven't missed any important points.

## Resolution

- **Implementation**
    - Updated `script/init.sql`: removed `idx_cmc_fgi_score` and `idx_cmc_fgi_name`; retained `idx_cmc_fgi_timestamp`.
- **Documentation updates** (`doc/timescaledb-production-setup.md`)
    - **Indexes**: removed selectivity helpers `idx_cmc_fgi_score` and `idx_cmc_fgi_name`.
    - **Tables and hypertables**: ensured `crypto_scout.cmc_fgi (timestamp)` and both Bybit ticker tables are listed.
    - **Compression**: documented `cmc_fgi` compression (`compress_segmentby = 'name'`, `compress_orderby = 'timestamp DESC'`).
    - **Backups**: added `BACKUP_KEEP_WEEKS=4` to the example to match the retention settings described earlier.
- **Verification**
    - No SQL references remain to the removed indexes; reorder policy still uses `idx_cmc_fgi_timestamp`.
- **Migration note (existing DBs)**
    - Safe to drop obsolete indexes during a maintenance window:
    ```sql
    DROP INDEX IF EXISTS crypto_scout.idx_cmc_fgi_score;
    DROP INDEX IF EXISTS crypto_scout.idx_cmc_fgi_name;
    ```