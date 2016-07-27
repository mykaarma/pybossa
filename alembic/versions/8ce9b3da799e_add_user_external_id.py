"""Add user external ID

Revision ID: 8ce9b3da799e
Revises: 4f12d8650050
Create Date: 2016-07-27 12:12:46.392252

"""

# revision identifiers, used by Alembic.
revision = '8ce9b3da799e'
down_revision = '4f12d8650050'

from alembic import op
import sqlalchemy as sa

field = 'external_uid'


def upgrade():
    op.add_column('task_run', sa.Column(field, sa.String))


def downgrade():
    op.drop_column('task_run', field)
