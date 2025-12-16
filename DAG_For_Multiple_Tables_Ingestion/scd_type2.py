def build_scd2_merge_sql(
    staging_table: str,
    target_table: str,
    primary_keys: list,
    business_keys: list,
    non_business_keys: list = None
):
    
    # ----------------------------
    # Validation
    # ----------------------------
    if not primary_keys:
        raise ValueError("primary_keys must not be empty")

    if not business_keys:
        raise ValueError("business_keys must not be empty")

    non_business_keys = non_business_keys or []

    # ----------------------------
    # Columns to SELECT / INSERT
    # ----------------------------
    # NOTE: primary_keys do NOT have to be part of business_keys
    all_columns = primary_keys + business_keys + non_business_keys

    # remove duplicates while preserving order
    seen = set()
    all_columns = [c for c in all_columns if not (c in seen or seen.add(c))]

    select_expr = ",\n    ".join([f"S.{c}" for c in all_columns])
    insert_cols = ", ".join(all_columns)

    # ----------------------------
    # JOIN condition (supports composite PK)
    # ----------------------------
    pk_join_cond = " AND ".join([f"T.{pk} = S.{pk}" for pk in primary_keys])

    # ----------------------------
    # HASH expression (BUSINESS KEYS ONLY)
    # ----------------------------
    hash_expr = (
        "TO_HEX(MD5(CONCAT("
        + ", '|', ".join([f"CAST(S.{c} AS STRING)" for c in business_keys])
        + ")))"
    )

    # ============================================================
    # STEP 1 — Expire CHANGED current rows
    # ============================================================
    expire_sql = f"""
    MERGE `{target_table}` T
    USING (
      SELECT
        {select_expr},
        {hash_expr} AS src_row_hash
      FROM `{staging_table}` S
    ) S
    ON {pk_join_cond}
    AND T.is_current = TRUE

    WHEN MATCHED
    AND T.row_hash != S.src_row_hash
    THEN UPDATE SET
      T.is_current = FALSE,
      T.end_date = CURRENT_DATE(),
      T.record_check_ts = CURRENT_TIMESTAMP()
    """

    # ============================================================
    # STEP 2 — Insert NEW + CHANGED rows (new current records)
    # ============================================================
    insert_sql = f"""
    INSERT INTO `{target_table}` (
      {insert_cols},
      start_date,
      end_date,
      is_current,
      row_hash,
      record_check_ts
    )
    SELECT
      {select_expr},
      CURRENT_DATE()      AS start_date,
      NULL                AS end_date,
      TRUE                AS is_current,
      {hash_expr}         AS row_hash,
      CURRENT_TIMESTAMP() AS record_check_ts
    FROM `{staging_table}` S
    LEFT JOIN `{target_table}` T
      ON {pk_join_cond}
     AND T.is_current = TRUE
    WHERE
      {" OR ".join([f"T.{pk} IS NULL" for pk in primary_keys])}
      OR T.row_hash != {hash_expr}
    """

    # ============================================================
    # STEP 3 — Touch UNCHANGED current rows (reconciliation)
    # ============================================================
    touch_sql = f"""
    UPDATE `{target_table}` T
    SET record_check_ts = CURRENT_TIMESTAMP()
    FROM `{staging_table}` S
    WHERE
      {pk_join_cond}
      AND T.is_current = TRUE
      AND T.row_hash = {hash_expr}
    """

    return {
        "expire_sql": expire_sql.strip(),
        "insert_sql": insert_sql.strip(),
        "touch_sql": touch_sql.strip(),
    }