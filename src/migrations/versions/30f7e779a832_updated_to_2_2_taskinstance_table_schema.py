"""updated to 2.2 taskinstance table schema

Revision ID: 30f7e779a832
Revises: a344bc90fe1d
Create Date: 2021-11-15 17:17:11.192806

"""
from alembic import op
import sqlalchemy as sa
import airflow


# revision identifiers, used by Alembic.
revision = '30f7e779a832'
down_revision = 'a344bc90fe1d'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    # No way but deleting everything
    op.drop_table('ergo_job')
    op.drop_table('ergo_task')
    
    op.create_table('ergo_task',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('task_id', sa.String(length=128), nullable=False),
    sa.Column('request_data', sa.Text(), nullable=True),
    sa.Column('queue_url', sa.String(length=256), nullable=False),
    sa.Column('state', sa.String(length=20), nullable=False),
    sa.Column('created_at', airflow.utils.sqlalchemy.UtcDateTime(timezone=True), nullable=False),
    sa.Column('updated_at', airflow.utils.sqlalchemy.UtcDateTime(timezone=True), nullable=False),
    sa.Column('ti_task_id', sa.String(length=250), nullable=False),
    sa.Column('ti_dag_id', sa.String(length=250), nullable=False),
    sa.Column('ti_run_id', sa.String(length=250), nullable=False),
    sa.ForeignKeyConstraint(['ti_task_id', 'ti_dag_id', 'ti_run_id'], ['task_instance.task_id', 'task_instance.dag_id', 'task_instance.run_id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('ti_task_id', 'ti_dag_id', 'ti_run_id', name='ix_unique_task_instance')
    )
    op.create_index(op.f('ix_ergo_task_created_at'), 'ergo_task', ['created_at'], unique=False)
    op.create_index(op.f('ix_ergo_task_updated_at'), 'ergo_task', ['updated_at'], unique=False)
    op.create_table('ergo_job',
    sa.Column('id', sa.String(length=128), nullable=False),
    sa.Column('task_id', sa.Integer(), nullable=False),
    sa.Column('result_data', sa.Text(), nullable=True),
    sa.Column('result_code', sa.Integer(), nullable=False),
    sa.Column('error_msg', sa.Text(), nullable=True),
    sa.Column('created_at', airflow.utils.sqlalchemy.UtcDateTime(timezone=True), nullable=False),
    sa.Column('response_at', airflow.utils.sqlalchemy.UtcDateTime(timezone=True), nullable=True),
    sa.ForeignKeyConstraint(['task_id'], ['ergo_task.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_ergo_job_created_at'), 'ergo_job', ['created_at'], unique=False)
    op.create_index(op.f('ix_ergo_job_task_id'), 'ergo_job', ['task_id'], unique=True)
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f('ix_ergo_job_task_id'), table_name='ergo_job')
    op.drop_index(op.f('ix_ergo_job_created_at'), table_name='ergo_job')
    op.drop_table('ergo_job')
    op.drop_index(op.f('ix_ergo_task_updated_at'), table_name='ergo_task')
    op.drop_index(op.f('ix_ergo_task_created_at'), table_name='ergo_task')
    op.drop_table('ergo_task')
    # ### end Alembic commands ###
