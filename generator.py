import yaml
import numpy as np
from faker import Faker

USE_CASE_CONFIG_PATH = 'use_case_config.yaml'

def load_config(path=USE_CASE_CONFIG_PATH):
    with open(path, 'r') as f:
        return yaml.safe_load(f)

def generate_data(config, n_rows=1000, seed=42):
    fake = Faker()
    if seed is not None:
        fake.seed(seed)
        np.random.seed(seed)
    fields = config['use-case']['schema']['fields']
    data = {}
    for field in fields:
        name = field['name']
        ftype = field['type']
        



if __name__ == "__main__":
    config = load_config()
    print(config)