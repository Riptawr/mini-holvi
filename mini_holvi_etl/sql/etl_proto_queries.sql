CREATE TABLE facts_user (tracking_uuid uuid, mobile_verified bool, email_verified bool,
 identity_verified bool, invited bool, country text);

INSERT INTO facts_user
(SELECT tracking_uuid, mobile_verified, email_verified, identity_verified, invited, country
FROM core_user LEFT JOIN auth_user a ON core_user.user_ptr_id = a.id);


CREATE TABLE facts_company
(creator int, creator_tracking_uuid uuid, trade_name text, verified bool, domicile text);

INSERT INTO facts_company
(SELECT creator_id AS creator,
 u.tracking_uuid AS creator_tracking_uuid,
 trade_name,
 verified,
 domicile
FROM core_company AS company
  LEFT JOIN core_user u ON company.creator_id = u.user_ptr_id);


CREATE TABLE facts_account
(creator int, company int, tracking_uuid uuid,
 creator_tracking_uuid uuid, handle text, archived bool, domicile text);

INSERT INTO facts_account
(SELECT ca.creator_id as creator, ca.company_id as company,
 ca.tracking_uuid,
 u.tracking_uuid as creator_tracking_uuid,
 ca.handle, ca.archived, ca.domicile
FROM core_account ca
LEFT JOIN core_user u ON ca.creator_id = u.user_ptr_id);


CREATE TABLE facts_revenue
(account int, company int, feature text,
 timestamp_paid timestamptz, amount numeric,
 account_domicile text, account_tracking_uuid uuid);

INSERT INTO facts_revenue (
  SELECT
    cr.account_id AS account,
    a.company_id AS company,
    cr.feature,
    cr.timestamp_paid AS timestamp_paid,
    cr.amount,
    a.domicile AS account_domicile,
    a.tracking_uuid AS account_tracking_uuid
FROM core_revenue AS cr LEFT JOIN core_account a ON cr.account_id = a.id);

