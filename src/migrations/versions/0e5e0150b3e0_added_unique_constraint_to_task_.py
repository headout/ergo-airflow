"""Added unique constraint to task instance FK

Revision ID: 0e5e0150b3e0
Revises: d0023e077cbc
Create Date: 2020-07-15 12:04:02.843191

"""
from alembic import op
import sqlalchemy as sa
import airflow
from sqlalchemy.dialects import mysql

# revision identifiers, used by Alembic.
revision = '0e5e0150b3e0'
down_revision = 'd0023e077cbc'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    #op.create_unique_constraint('ix_unique_task_instance', 'ergo_task', ['ti_task_id', 'ti_dag_id', 'ti_execution_date'])
    op.create_unique_constraint("ti_unique_task_instance, 'task_instance', ['task_id', 'dag_id'])
    op.create_unique_constraint('ix_unique_task_instance', 'ergo_task', ['ti_task_id', 'ti_dag_id'])
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint('ix_unique_task_instance', 'ergo_task', type_='unique')
    # ### end Alembic commands ###
