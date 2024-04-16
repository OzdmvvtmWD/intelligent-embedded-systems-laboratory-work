import asyncio
import json
from sqlalchemy.exc import SQLAlchemyError

from typing import Set, Dict, List, Any
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, Body
from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    Integer,
    String,
    Float,
    DateTime,
)
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import select

#personal additions
from sqlalchemy import insert,delete,update
from sqlalchemy.sql.expression import func

from datetime import datetime
from pydantic import BaseModel, field_validator
from config import (
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_DB,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
)

# FastAPI app setup
app = FastAPI()
# SQLAlchemy setup
DATABASE_URL = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
engine = create_engine(DATABASE_URL)
metadata = MetaData()
# Define the ProcessedAgentData table
processed_agent_data = Table(
    "processed_agent_data",
    metadata,
    Column("id", Integer, primary_key=True, index=True),
    Column("road_state", String),
    Column("user_id", Integer),
    Column("x", Float),
    Column("y", Float),
    Column("z", Float),
    Column("latitude", Float),
    Column("longitude", Float),
    Column("timestamp", DateTime),
)

# SQLAlchemy model
class ProcessedAgentDataInDB(BaseModel):
    id: int
    road_state: str
    user_id: int
    x: float
    y: float
    z: float
    latitude: float
    longitude: float
    timestamp: datetime

class DataRow:
    pass

SessionLocal = sessionmaker(bind=engine)

# FastAPI models
class AccelerometerData(BaseModel):
    x: float
    y: float
    z: float


class GpsData(BaseModel):
    latitude: float
    longitude: float


class AgentData(BaseModel):
    user_id: int
    accelerometer: AccelerometerData
    gps: GpsData
    timestamp: datetime

    @classmethod
    @field_validator("timestamp", mode="before")
    def check_timestamp(cls, value):
        if isinstance(value, datetime):
            return value
        try:
            return datetime.fromisoformat(value)
        except (TypeError, ValueError):
            raise ValueError(
                "Invalid timestamp format. Expected ISO 8601 format (YYYY-MM-DDTHH:MM:SSZ)."
            )


class ProcessedAgentData(BaseModel):
    road_state: str
    agent_data: AgentData




# WebSocket subscriptions
subscriptions: Dict[int, Set[WebSocket]] = {}


# FastAPI WebSocket endpoint
@app.websocket("/ws/{user_id}")
async def websocket_endpoint(websocket: WebSocket, user_id: int):
    await websocket.accept()
    if user_id not in subscriptions:
        subscriptions[user_id] = set()
    subscriptions[user_id].add(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        subscriptions[user_id].remove(websocket)


# Function to send data to subscribed users
async def send_data_to_subscribers(user_id: int, data):
    if user_id in subscriptions:
        for websocket in subscriptions[user_id]:
            await websocket.send_json(json.dumps(data))


# FastAPI CRUDL endpoints

       
@app.post("/processed_agent_data/")
async def create_processed_agent_data(data: List[ProcessedAgentData]):
    try:
        db = SessionLocal()
        for item in data:
            agent_data = item.agent_data
            new_entry = processed_agent_data.insert().values(
                road_state=item.road_state,
                user_id=agent_data.user_id,
                x=agent_data.accelerometer.x,
                y=agent_data.accelerometer.y,
                z=agent_data.accelerometer.z,
                latitude=agent_data.gps.latitude,
                longitude=agent_data.gps.longitude,
                timestamp=agent_data.timestamp,
            )
            db.execute(new_entry)
        db.commit()
        return {"message": "Data inserted successfully"}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail="Internal Server Error")
    finally:
        db.close()    


@app.get(
    "/processed_agent_data/{processed_agent_data_id}",
    response_model=ProcessedAgentDataInDB,
)
def read_processed_agent_data(processed_agent_data_id: int):
    try:
        db = SessionLocal()
        return db.query(processed_agent_data).filter(processed_agent_data.c.id == processed_agent_data_id).first()
    except SQLAlchemyError as e:
        return {"error": str(e)}

@app.get("/processed_agent_data/", response_model=list[ProcessedAgentDataInDB])
def list_processed_agent_data():
    try:
        db = SessionLocal()
        return db.query(processed_agent_data).all()
    except SQLAlchemyError as e:
        return {"error": str(e)}

@app.put(
    "/processed_agent_data/{processed_agent_data_id}",
    response_model=ProcessedAgentDataInDB,
)
def update_processed_agent_data(processed_agent_data_id: int, data: ProcessedAgentData):
    try:
        # Update data
        db = SessionLocal()
        update_data = ProcessedAgentDataInDB(
            id=processed_agent_data_id,
            road_state=data.road_state,
            user_id=data.agent_data.user_id,
            x=data.agent_data.accelerometer.x,
            y=data.agent_data.accelerometer.y,
            z=data.agent_data.accelerometer.z,
            latitude=data.agent_data.gps.latitude,
            longitude=data.agent_data.gps.longitude,
            timestamp=data.agent_data.timestamp
        )
        update_db = update(processed_agent_data).where(processed_agent_data.c.id == processed_agent_data_id).values(update_data.model_dump()).returning(processed_agent_data.c)
        db.execute(update_db)
        db.commit()
        updated_data = db.execute(select(processed_agent_data).filter(processed_agent_data.c.id == processed_agent_data_id)).fetchone()
        return updated_data
    except SQLAlchemyError as e:
        db.rollback()
        return {"error": str(e)}


@app.delete(
    "/processed_agent_data/{processed_agent_data_id}",
    response_model=ProcessedAgentDataInDB,
)
def delete_processed_agent_data(processed_agent_data_id: int):
    try:
        db = SessionLocal()
        db_item = delete(processed_agent_data).where(processed_agent_data.c.id == processed_agent_data_id).returning(processed_agent_data.c)
        deleted_item = db.execute(db_item).fetchone()
        db.commit()
        return deleted_item
    except SQLAlchemyError as e:
        db.rollback()
        return {"error": str(e)}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000)
