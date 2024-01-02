INSERT INTO dgdm_{tenant}.dim_wrap_up_codes
    SELECT id as wrapUpId, name wrapUpCode FROM gpc_{tenant}.raw_wrapup_codes