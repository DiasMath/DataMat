#!/usr/bin/env python3
from __future__ import annotations
import os, sys, json, time
from pathlib import Path
from .test_utils import load_test_envs # <-- Atualizado

def simulate_daily_execution(client_id: str):
    print(f"🌅 Simulando execução diária para {client_id}")
    load_test_envs(client_id) # <-- Atualizado
    # ... (o resto do código do arquivo permanece o mesmo)
    # ...