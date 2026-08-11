"""20260811_0001_create_mng_table

Revision ID: ff9d247bb84b
Revises: 
Create Date: 2026-08-11 22:13:05.143679

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'ff9d247bb84b'
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE mng_files (
            file_name     text        PRIMARY KEY,
            market        text        NOT NULL,
            updated_at    timestamptz NOT NULL
        )
    """)


def downgrade() -> None:
    op.execute("DROP TABLE mng_files")
