# -*- coding: utf-8 -*-
import enum

import const

# Database 관련 Constants
const.RDS_HOST = "rds-candipay.cxjgh9mqa3mo.ap-northeast-2.rds.amazonaws.com"
const.RDS_USERNAME = "balance_app"
const.RDS_PASSWORD = "1"
const.RDS_DB_NAME = "balance"


# 거래관련 Constants
class TransactionCategory(str, enum.Enum):
    """
    내부적으로 다뤄지는 거래분류를 정의합니다
    """

    NORMAL = "NORMAL"
    CANCEL = "CANCEL"
    NW_CANCEL = "NW_CANCEL"


class TransactionType(str, enum.Enum):
    """
    내부적으로 다뤄지는 거래유형를 정의합니다
    """

    USE = "USE"
    CHARGE = "CHARGE"
    TRANSFER_SEND = "TRANSFER_SEND"
    TRANSFER_RECV = "TRANSFER_RECV"

TX_SIGN = {
    "normal": {"use": "-", "charge": "+", "trans_send": "-", "trans_recv": "+"},
    "cancel": {"use": "+", "charge": "-"},
    "nw-cancel": {"use": "+", "charge": "-"}
}
