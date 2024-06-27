# coding: utf-8
import random
import time
from scrappeFonctions import allLinks
from selenium.webdriver.common.by import By
from linkedinScrapper2 import actions
from scrappeFonctions import people,credentials as cred
from scrappeFonctions.paths import Paths
import numpy as np
import pandas as pd
from scrappeFonctions.getPeopleLinkedinId import *

import json
cred = cred.Credentials()
spreadsheets = "Data Sales"
worksheet = "Sales info"

index = 0
def login(email, pwd):
    driver = Paths().driver
    actions.login(driver=driver, email=email, password=pwd)
    driver.maximize_window()
    return driver


def groupLoginV2(creds):
    llo = creds
    loggers = []
    index = []
    for i in tqdm(range(len(llo)), desc="Logging... "):
        try:
            log = login(email=llo[i][0], pwd=llo[i][1])
            time.sleep(5)
            if not ("temporairement restreint" in log.page_source):
                loggers.append(log)
                index.append(i)
            else:
                log.close()
        except:
            pass
    return loggers, index



def updateFieldScrapped(dfDict, logger, worksheet, spreadsheets):
    logger = str(logger)
    timeStamp = datetime.now().strftime("%Y-%m-%d %H-%M-%S")

    if type(dfDict) == dict:
        df = pd.DataFrame({k: pd.Series(v) for k, v in dfDict.items()})
        if len(df) == 0:
            df = pd.DataFrame.from_dict(dfDict)
    else:
        df = dfDict

    if len(df) >= 1:
        fileName = f"stockageCSV/{logger}__{spreadsheets}__{worksheet}__{timeStamp}_NRow_{len(df)}.csv"
        df.to_csv(path_or_buf=fileName, index=False)
    return len(df)


def startScrapping(logger, driver, i):
    global salesDFinfos, loggers, mainCols, itUpdated, myWKL, nScrappeDone, onProcess, fromHubspot
    global names, compIDS, personNames, personIDS, personLinks, personJobTitle, personJobLocation, index, salesInfosList
    f = [names, compIDS, personNames, personIDS, personLinks, personJobTitle, personJobLocation]
    salesInfos = {'People ID': [], 'Linkedin ID': [], 'People name': [], 'jobTitle': [], 'People LinkedIn': [],
                  'mediaLink': [], 'summary': [], 'headline': [], 'profilIndustrie': [],
                  'jobLocation': [], 'Formation': [], 'skills': [], "abonneeCount": [], "relation": [],
                  'Company name': [], 'Company ID': [], "GoodLink": [], 'logger': [], 'source': [], 'timestamp': [],
                  "soup": []}
    index = i
    itUpdated = False
    myCoumpt = 0

    myIndex = 0
    failedTime = time.time()
    faildCount = 0
    myFaild = 0
    exps = []
    forms = []

    def ingestionDeInfoMerge(oneInfo, columns, goodLinkStatus):
        infoMerge = {k: '' for k in columns}
        headers = {k: n[myIndexBis] for n, k in zip(f, salesDFLink.keys())}
        headers["logger"] = logger
        headers["source"] = "LinkedIn"
        headers["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        headers["GoodLink"] = goodLinkStatus
        for key, value in headers.items():
            infoMerge[key] = value
        if not goodLinkStatus:
            for key in salesInfos.keys():
                if key not in headers.keys():
                    if "abonneeCount" not in columns:
                        for val_i in range(10):
                            infoMerge[f"{key}_{val_i + 1}"] = ""
                    else:
                        infoMerge[key] = ""
        else:
            for key, value in oneInfo.items():
                if key not in headers.keys():
                    if "abonneeCount" not in columns:
                        for val_i in range(10):
                            val = ""
                            try:
                                val = value[val_i]
                            except:
                                pass
                            if key == "Company ID" and val != "":
                                val = f"https://www.linkedin.com/company/{val}"
                            infoMerge[f"{key}_{val_i + 1}"] = val
                    else:
                        if type(value) == list:
                            if value != []:
                                infoMerge[key] = value[0]
                            else:
                                infoMerge[key] = ""
                        else:
                            infoMerge[key] = value
        return infoMerge

    def tryUpdating(salesInformation, log, work, spreadsheet):
        cols = salesInformation.keys()
        while itUpdated:
            print("Not ready yet")
            time.sleep(random.randint(3, 10))
        try:
            # dfSalesInfo = pd.DataFrame.from_dict(salesInformation)
            updateFieldScrapped(salesInformation, log, work, spreadsheet)
        except:
            lastLen = len(salesInformation['People LinkedIn'])
            for k, val in salesInformation.items():
                if lastLen != len(val):
                    print(k, len(val))
                    if lastLen > len(val):
                        for _ in range(lastLen - len(val)):
                            salesInformation[k].setdefault(k, []).append("")
                lastLen = len(salesInformation[k])
            try:
                updateFieldScrapped(salesInformation, log, work, spreadsheet)
            except:
                time.sleep(10)
                try:
                    updateFieldScrapped(salesInformation, log, work, spreadsheet)
                except Exception as e:
                    print("Grosse Erreur de sauvegarde : ", str(e))
                    pass
        salesInformation = {k: [] for k in cols}
        return salesInformation

    oldLinkAlreadyScrappe = []
    oldIDAlreadyScrappe = []
    if len(salesDFinfos) > 1:
        oldLinkAlreadyScrappe = salesDFinfos["People LinkedIn"].values.tolist()
    llen = len(personLinks)
    if not fromHubspot:
        llen -= len(list(set(oldLinkAlreadyScrappe)))
    pbar = tqdm(total=int(llen / len(logger)) + 1, desc=f"{logger} ==> Scrapping People Info")
    # Debut du chrono
    tempsTotal = []
    while index < len(personLinks):
        myIndexBis = index
        index += 1
        i += 1
        link = personLinks[myIndexBis]
        peopleId = personIDS[myIndexBis]
        goodLink = True
        parmelink = link.split("https://www.linkedin.com/in/")[-1].split("/")[0]
        link = f"https://www.linkedin.com/in/{parmelink}/"
        try:
            pIds = personIDS[myIndexBis]
            pName = personNames[myIndexBis]
        except:
            print("Error with index ", myIndexBis, link)
            pIds = ""
            pName = ""
        if pIds == "":
            personIDS[myIndexBis] = parmelink
        canScrap = True
        if link not in onProcess:
            onProcess.append(link)
            if canScrap:
                pbar.update(n=1)
                print(f"{logger} ==> {pIds} at {datetime.now()}")
                try:
                    link = link.replace(" A", "A")
                    driver.get(link)
                    driver.implicitly_wait(random.randint(5, 10))
                    sourcepage = driver.page_source
                    oneInfos = ingestionPeopleData(logger, driver, sourcepage)
                    try:
                        if oneInfos != {}:
                            for ketInfo, vfInfo in ingestionDeInfoMerge(oneInfos, mainCols, goodLink).items():
                                salesInfos.setdefault(ketInfo, []).append(vfInfo)
                    except Exception as e:
                        print("Info", str(e))

                    if (myIndexBis % 10 == 0 and len(salesInfos["jobTitle"]) > 0) or (len(loggers) == 1):
                        salesInfos = tryUpdating(salesInfos, logger, worksheet, spreadsheets)
                    itUpdated = False

                    if faildCount > 0:
                        faildCount -= 1
                except Exception as e:
                    print(str(e))
                randTimeSleep = random.randint(10, 60)
                print("Temps d'attente : ", randTimeSleep)
                tempsTotal.append(randTimeSleep)
                time.sleep(randTimeSleep)

            if myIndexBis % 50 == 0 and len(salesInfos["jobTitle"]) > 0:
                salesInfos = tryUpdating(salesInfos, logger, worksheet, spreadsheets)
    pbar.close()
    salesInfos = tryUpdating(salesInfos, logger, worksheet, spreadsheets)
    return salesInfos


names, compIDS, personNames, personIDS, personLinks, personJobTitle, personJobLocation, salesDFLink, salesDFinfo = [], [], [], [], [], [], [], [], []
salesDFinfos, mainCols, salesInfosList = [], [], []
onProcess = []
onFailed = []
nScrappeDone = 0
itUpdated = True
fromHubspot = False
llo = cred.fakeCredsWithSaleNave[:1]
# llo = [cred.jeremy,cred.djibril,cred.djibrilTrue]
people.oneTime = False
loggers = []


def startScrap(llo, loggers, links=None):
    global names, compIDS, personNames, personIDS, personLinks, personJobTitle, personJobLocation, salesDFLink, \
        salesDFinfo, salesDFinfos, index, onProcess, onFailed, mainCols, salesInfosList

    df = pd.read_csv("liste_liens_sales_Software_scrapped.csv")
    if links is not None:
        df = df[df["People LinkedIn"].isin(links["links"])]
        notIn = links[links["links"].isin(df["People LinkedIn"])]
        notIn = notIn.rename(columns={"links": "People LinkedIn"})
        for k in df.keys():
            if k not in notIn.keys():
                notIn[k] = ""
        notIn["People ID"] = notIn["People LinkedIn"].apply(
            lambda v: str(v).split("https://www.linkedin.com/in/")[1].replace("/", ""))
    index = 0
    maj = fromHubspot  # Pour mettre à jours toute la base
    onProcess = []
    onFailed = []
    nScrappeDone = 0
    itUpdated = True
    GsalesInfos, mainCols, salesDFLink, salesDFinfos = initContenaire(salesDFLink=df)
    salesInfosList = [GsalesInfos for _ in range(len(llo))]
    names, compIDS, personNames, personIDS, personLinks, personJobTitle, personJobLocation, salesDFLink, salesDFinfo = initValues(
        salesDFLink, salesDFinfos, maj=maj)
    if loggers == []:
        loggers, indices = groupLoginV2(llo)
        llo = [llo[loi] for loi in indices]

    startScrapping(llo[0][0].split("@")[0], loggers[0], 0)


# time.sleep(6 * 60 * 60)

startScrap(llo, loggers)
