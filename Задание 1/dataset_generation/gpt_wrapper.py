import requests
from time import sleep
from typing import Optional

def retry_yandex_gpt_factory(retries=2):
    def retry_yandex_gpt(func):
        def wrapper_retry_yandex_gpt(*args, **kwargs):
            for retry in range(retries):
                res = func(*args, **kwargs)
                if (res.status_code) == 200:
                    return res.json()['result']['alternatives'][0]['message']['text']
                else:
                    print(f"Request failed {res.status_code}: {res.json()}, retry number: {retry + 1}")
                    if res.status_code == 429:
                        sleep(5)
        return wrapper_retry_yandex_gpt
    return retry_yandex_gpt

class YandexGptApi:
    def __init__(self, oauth_token: str, folder_id: str):
        self.oauth_token = oauth_token
        self.folder_id = folder_id
        self.model_url = "https://llm.api.cloud.yandex.net/foundationModels/v1/completion"
        self.iam_token = self.__get_iam_token(oauth_token)

    @staticmethod
    def __get_iam_token(oauth_token: str) -> str:
        url = "https://iam.api.cloud.yandex.net/iam/v1/tokens"
        data = {"yandexPassportOauthToken": oauth_token}
        headers = {'Content-Type': 'application/json'}
        response = requests.post(url, json=data, headers=headers).json()
        return response['iamToken']

    @retry_yandex_gpt_factory(5)
    def completion(self, system_prompt: str, user_prompt: str, temperature: float = 0.6, max_tokens: int = 2000):
        prompt = {
            "modelUri": f"gpt://{self.folder_id}/yandexgpt/latest",
            "completionOptions": {
                "stream": False,
                "temperature": temperature,
                "maxTokens": max_tokens
            },
            "messages": [
                {"role": "system", "text": system_prompt},
                {"role": "user", "text": user_prompt}
            ]
        }
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.iam_token}",
            "x-folder-id": self.folder_id
        }
        return requests.post(self.model_url, headers=headers, json=prompt)


def insert_fake_add(gpt: YandexGptApi, text: str) -> Optional[str]:
    user_prompt = f'Придумай текст рекламы какого-нибудь продукта, используя данные из этой новостной статьи. \
        Самостоятельно придумай релевантный продукт и название бренда.\n{text}'
    system_prompt = """Ответ должен быть в таком формате:

    Бренд: <название придуманного бренда>
    Продукт: <названиие придуманного продукта>
    Реклама: <текст c рекламой продукта>
    """

    text = gpt.completion(system_prompt, user_prompt)
    if not text or 'Реклама:' not in text:
        return None

    ad_text = text.split('Реклама:')[1]
    processed_ad_text = []
    prev_symb = '\n'
    for symb in ad_text:
        if not ((symb == '*') or (symb == '\n' and prev_symb == '\n')):
            processed_ad_text.append(symb)
        prev_symb = symb
    return ''.join(processed_ad_text)
