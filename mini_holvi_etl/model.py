from typing import Optional, Dict


class EtlQuery:
    def __init__(self, params: Optional[Dict[str, str]]=None):
        if params is None:
            params = {"start_id": "1"}
        self._params = params

    def dml(self) -> str:
        pass


class core_user(EtlQuery):
    def dml(self): return ("SELECT tracking_uuid, mobile_verified, email_verified, identity_verified, invited, country\n"
                           f"FROM core_user cu LEFT JOIN auth_user a ON cu.user_ptr_id = a.id AND a.id >= {self._params.get('start_id')};")


class core_company(EtlQuery):
    def dml(self): return ("SELECT creator_id AS creator,\n"
                           " u.tracking_uuid AS creator_tracking_uuid,\n"
                           " trade_name,\n"
                           " verified,\n"
                           " domicile\n"
                           "FROM core_company AS company\n"
                           f"LEFT JOIN core_user u ON company.creator_id = u.user_ptr_id AND company.id >= {self._params.get('start_id')};")


class core_account(EtlQuery):
    def dml(self): return ("SELECT ca.creator_id as creator, ca.company_id as company,\n"
                           " ca.tracking_uuid,\n"
                           " u.tracking_uuid as creator_tracking_uuid,\n"
                           " ca.handle, ca.archived, ca.domicile\n"
                           "FROM core_account ca\n"
                           f"LEFT JOIN core_user u ON ca.creator_id = u.user_ptr_id AND ca.id >= {self._params.get('start_id')};")


class core_revenue(EtlQuery):
    def dml(self): return ("  SELECT\n"
                           "    cr.account_id AS account,\n"
                           "    a.company_id AS company,\n"
                           "    cr.feature,\n"
                           "    cr.timestamp_paid AS timestamp_paid,\n"
                           "    cr.amount,\n"
                           "    a.domicile AS account_domicile,\n"
                           "    a.tracking_uuid AS account_tracking_uuid\n"
                           f"FROM core_revenue AS cr LEFT JOIN core_account a ON cr.account_id = a.id AND cr.id >= {self._params.get('start_id')};")