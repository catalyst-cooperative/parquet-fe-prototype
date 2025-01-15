"""DB model definitions."""

from flask_login import UserMixin

from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.orm import Mapped, mapped_column

db = SQLAlchemy()


class User(UserMixin, db.Model):
    """It's the user.

    OK, so the auth0_id is a unique ID that comes from auth0. This lets us figure out
    if the user's been created before, and also lets us avoid getting weird
    duplicate-key errors if e.g. email is re-used.
    """

    id: Mapped[int] = mapped_column(primary_key=True)
    auth0_id: Mapped[str] = mapped_column(unique=True)
    username: Mapped[str]
    email: Mapped[str]

    @staticmethod
    def get(user_id):
        return User.query.get(int(user_id))

    @staticmethod
    def from_userinfo(userinfo):
        return User(
            auth0_id=userinfo["sub"],
            email=userinfo["email"],
            username=userinfo.get(
                "preferred_username", userinfo["email"].split("@")[0]
            ),
        )
