## Requirements

pip install -r requirements.txt

## Docker

docker-compose up -d

## Uvicorn

echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.bashrc
source ~/.bashrc


uvicorn api:api --reload

## Streamlit

streamlit run streamlit_app/Home.py
