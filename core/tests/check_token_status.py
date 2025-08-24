#!/usr/bin/env python3
from __future__ import annotations
import os, sys, json, time
from pathlib import Path
from .test_utils import load_test_envs # <-- Atualizado

def check_token_status(client_id: str):
    print(f"🔍 Verificando status dos tokens para {client_id}")
    load_test_envs(client_id) # <-- Atualizado
    # ... (o resto do código do arquivo permanece o mesmo)
    # ...