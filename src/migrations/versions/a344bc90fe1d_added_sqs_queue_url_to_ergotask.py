"""Added SQS queue_url to ErgoTask

Revision ID: a344bc90fe1d
Revises: 0e5e0150b3e0
Create Date: 2020-08-18 18:23:39.553722

"""
from alembic import op
import sqlalchemy as sa
import airflow


# revision identifiers, used by Alembic.
revision = 'a344bc90fe1d'
down_revision = '0e5e0150b3e0'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('ergo_task', sa.Column('queue_url', sa.String(length=256), nullable=False))
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('ergo_task', 'queue_url')
    # ### end Alembic commands ###
