"""${message}
"""
import re
from typing import Any

${imports}

revision = '${revision}'
down_revision = ${down_revision}
branch_labels = ${branch_labels}
depends_on = ${depends_on}


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
