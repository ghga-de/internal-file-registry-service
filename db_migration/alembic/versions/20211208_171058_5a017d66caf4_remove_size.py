"""remove size

Revision ID: 5a017d66caf4
Revises: 3b0661f98079
Create Date: 2021-12-08 17:10:58.160439

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "5a017d66caf4"
down_revision = "3b0661f98079"
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column("fileinfo", "size")
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column(
        "fileinfo", sa.Column("size", sa.INTEGER(), autoincrement=False, nullable=False)
    )
    # ### end Alembic commands ###