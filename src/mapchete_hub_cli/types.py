from typing import Optional

from pydantic import BaseModel


class Progress(BaseModel):
    current: int = 0
    total: Optional[int] = None
