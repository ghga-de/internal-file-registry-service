"""renamed external_id to file_id

Revision ID: 826d7777c67c
Revises: 5a017d66caf4
Create Date: 2021-12-09 14:59:16.262616

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "826d7777c67c"
down_revision = "5a017d66caf4"
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column("fileinfo", sa.Column("file_id", sa.String(), nullable=False))
    op.drop_constraint("fileinfo_external_id_key", "fileinfo", type_="unique")
    op.create_unique_constraint(None, "fileinfo", ["file_id"])
    op.drop_column("fileinfo", "external_id")
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column(
        "fileinfo",
        sa.Column("external_id", sa.VARCHAR(), autoincrement=False, nullable=False),
    )
    op.drop_constraint(None, "fileinfo", type_="unique")
    op.create_unique_constraint("fileinfo_external_id_key", "fileinfo", ["external_id"])
    op.drop_column("fileinfo", "file_id")
    # ### end Alembic commands ###
