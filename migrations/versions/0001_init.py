"""Create durable job, attempt and audit tables.
Revision ID: 0001
Revises:
"""
from alembic import op
from asyncforge.db import Base
revision = "0001"
down_revision = None
branch_labels = None
depends_on = None

def upgrade():
    Base.metadata.create_all(op.get_bind(), checkfirst=False)

def downgrade():
    Base.metadata.drop_all(op.get_bind(), checkfirst=False)
