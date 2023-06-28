import asyncio

import requests


class CWebServicesClient:

    def __init__(self):
        self._BaseUrl = "http://localhost:5000"

    def SumValues(self, sCategory="", sIndustry=""):
        sUrl = "/".join([self._BaseUrl, "sum"])
        dBody = {
            "category": sCategory,
            "industry": sIndustry,
        }
        try:
            response = requests.post(url=sUrl, json=dBody)
            if response.ok:
                print(
                    f'{sUrl} successfully sent with the following params: category: "{sCategory}", '
                    f'industry: "{sIndustry}". response: "{response.json()}"'
                )
            else:
                raise BaseException(f"Failed to send {sUrl}")
        except BaseException as ex:
            print(f"Exception raised while trying to send {sUrl}. Exception: {str(ex)}")

    async def MakeApiRequest(self, sCategory, sIndustry):
        self.SumValues(sCategory=sCategory, sIndustry=sIndustry)

    async def MakeApiRequests(self, nRequests=5):
        sem = asyncio.Semaphore(nRequests)
        lCombinations = [
            ("", ""),
            ("Financial ratios", "Dairy Cattle Farming"),
            ("Financial position", ""),
            ("", "Dairy Cattle Farming"),
            ]

        lTasks = []
        async with sem:
            for cat_ind in lCombinations:
                task = asyncio.ensure_future(self.MakeApiRequest(sCategory=cat_ind[0], sIndustry=cat_ind[1]))
                lTasks.append(task)

            await asyncio.gather(*lTasks)
