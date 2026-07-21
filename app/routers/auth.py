from fastapi import APIRouter, HTTPException, status, Depends
from sqlalchemy.ext.asyncio import AsyncSession 
from sqlalchemy import select
from app.dependencies import get_db
from ..auth import create_access_token, create_refresh_token, verify_password, decode_token, hash_password
from ..models import UserResponse, UserCreate, TokenRefreshRequest, TokenResponse, NewAccessTokenResponse
from ..orm_models import UserORM

router = APIRouter(tags=["authentication"])

@router.post("/register", status_code=status.HTTP_201_CREATED, response_model=UserResponse)
async def register_user(user_in: UserCreate, db: AsyncSession = Depends(get_db)):
    query = select(UserORM).where(UserORM.username == user_in.username)
    result = await db.execute(query)
    if result.scalar_one_or_none():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="username already exists"
        )
    new_user = UserORM(
        username = user_in.username,
        hashed_password = hash_password(user_in.password),
        
    )
    db.add(new_user)
    await db.commit()
    await db.refresh(new_user)
    return new_user

@router.post("/login", status_code=status.HTTP_200_OK, response_model=TokenResponse)
async def login_user(user_in: UserCreate, db: AsyncSession = Depends(get_db)):
    query = select(UserORM).where(UserORM.username == user_in.username)
    result = await db.execute(query)
    user = result.scalar_one_or_none()


    if not user or not verify_password(user_in.password, user.hashed_password):
        raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Wrong username or password",
        )
    token_data = {"sub": user.username, "role": user.role}
    return {"access_token": create_access_token(token_data),
        "refresh_token": create_refresh_token(token_data)}

@router.post("/refresh", status_code=status.HTTP_200_OK, response_model=NewAccessTokenResponse)
async def new_access_token(body: TokenRefreshRequest, db: AsyncSession = Depends(get_db)):
    payload = decode_token(body.refresh_token)
    username = payload.get("sub")

    if not username:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid refresh token payload",
        )

    query = select(UserORM).where(UserORM.username == username)
    result = await db.execute(query)
    user = result.scalar_one_or_none()

    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found"
        )
    token_data = {"sub": user.username, "role": user.role}
    return {"access_token": create_access_token(token_data)}
