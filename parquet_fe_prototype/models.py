from flask_login import UserMixin

from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

db = SQLAlchemy()


class User(UserMixin, db.Model):
    id: Mapped[int] = mapped_column(primary_key=True)
    auth0_id: Mapped[str] = mapped_column(unique=True)
    username: Mapped[str] = mapped_column(unique=True)
    email: Mapped[str]

    @staticmethod
    def get(user_id):
        return User.query.get(int(user_id))

    @staticmethod
    def from_userinfo(userinfo):
        return User(
            auth0_id=userinfo["sub"],
            email=userinfo["email"],
            username=userinfo.get("preferred_username", userinfo["email"]),
        )
