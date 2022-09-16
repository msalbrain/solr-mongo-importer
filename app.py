from fastapi import FastAPI
from time import time

app = FastAPI()



d = {"num": 0, "time": int(time())}

from pydantic import BaseModel


class Item(BaseModel):
    raw_text: str
    text: str


@app.post("/hello")
async def rec_solr(val: list[Item]):

    print(val[0].json())
    d["num"] += 1
    d["time"] = int(time())
    print(d["num"])
    return d