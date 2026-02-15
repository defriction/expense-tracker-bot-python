from __future__ import annotations

import secrets
from typing import Optional

from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel, Field

from app.core.config import Settings
from app.services.repositories import DataRepo


class CreateInviteRequest(BaseModel):
    actor_user_id: Optional[str] = Field(default=None, max_length=64)


def _generate_invite_token() -> str:
    return f"INV-{secrets.token_urlsafe(18)}"


def build_admin_router(repo: DataRepo, settings: Settings) -> APIRouter:
    router = APIRouter(prefix="/admin", tags=["admin"])

    @router.post("/invites")
    async def create_invite(
        payload: Optional[CreateInviteRequest] = None,
        x_admin_api_key: Optional[str] = Header(default=None, alias="X-Admin-Api-Key"),
    ):
        configured_key = settings.invite_admin_api_key
        if not configured_key:
            raise HTTPException(status_code=503, detail="Invite admin API key not configured")
        if not x_admin_api_key or not secrets.compare_digest(x_admin_api_key, configured_key):
            raise HTTPException(status_code=401, detail="Unauthorized")

        actor_user_id = payload.actor_user_id if payload else None
        token = _generate_invite_token()
        invite = repo.create_invite(token, actor_user_id)
        return {
            "ok": True,
            "inviteToken": invite["inviteToken"],
            "status": invite["status"],
            "startCommand": f"/start {invite['inviteToken']}",
        }

    return router
