import time
from collections import Counter
from bs4 import BeautifulSoup

import pandas as pd, random

from datetime import datetime
from tqdm import tqdm
maj = False
oneTime = True
personExpTags, personFormTags = [], []
salesDFExp, salesDFForm, salesDFLink, personNames, personIDS, personLinks = pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), [], [], []



loggers = []
llo = []
scrappeExp = False
scrappeForm = False


def getByList(lItems, node=False):
    move = ['↧', '↥', '=']
    compNam = ""
    jobType = ""
    dateEmp = ""
    dureEmp = ""
    jobLocation = ""
    description = ""
    compLink = ""
    try:
        compLinkText = lItems.find("a")
        compLink = compLinkText.attrs["href"]
    except:
        pass
    # Ici c'est s'il n'y a pas des noeuds
    if not node:
        try:
            compName = lItems.find("span", {"class": "t-14 t-normal"}).find("span", {
                "class": "visually-hidden"}).text.strip()
            if " · " in compName:
                compNam = compName.split(" · ")[0]
                jobType = compName.split(" · ")[1]
            else:
                compNam = compName
        except:
            pass
        jobTitle = lItems.find("div", {"class": "display-flex flex-column full-width align-self-center"}).find(
            "div", {"class": "display-flex flex-wrap align-items-center full-height"}).find("div", {
            "class": "display-flex"}).find("span", {"class": "visually-hidden"}).text.strip()
    else:
        compName = lItems.find("div", {"class": "display-flex flex-column full-width align-self-center"}).find(
            "div", {"class": "display-flex"}).find("span", {"class": "visually-hidden"}).text.strip()
        try:
            jobType = lItems.find("span", {"class": "t-14 t-normal"}).find("span", {
                "class": "visually-hidden"}).text.strip().split(" ")[0]
        except:
            pass
        divs = lItems.find("div", {"class": "scaffold-finite-scroll__content"}).find("ul", {"class": "pvs-list"})

        divs_parents = divs.find_all("li", {"class": "pvs-list__paged-list-item pvs-list__item--one-column"})
        nodes = {}
        for d in divs_parents:
            noeud = getByList(d)
            for k, v in noeud.items():
                if k == "jobType":
                    nodes.setdefault(k, []).append(jobType)
                elif k == "Company name":
                    nodes.setdefault(k, []).append(compName)
                else:
                    nodes.setdefault(k, []).append(v)
        return nodes
    # Get the Date and location
    dateLocation = lItems.find_all("span", {"class": "t-14 t-normal t-black--light"})[:2]
    for dt in dateLocation:
        dat = dt.find("span", {"class": "pvs-entity__caption-wrapper"})
        if dat is not None:
            dateText = dat.text
            if " · " in dateText:
                dateEmp = dateText.split(" · ")[0]
                dureEmp = dateText.split(" · ")[1]
            else:
                dateEmp = dateText
        else:

            jobLocation = dt.find("span", {"class": "visually-hidden"}).text.strip()

    # Get the description
    try:
        description = lItems.find("div",
                                  {"class": "pvs-list__outer-container pvs-entity__sub-components"}).text.strip()
    except:
        pass
    curExp = {"jobTitle": jobTitle, "Company name": compNam,
              "Company page": compLink, "jobType": jobType,
              "Description": description,
              "Dates d'emploi": dateEmp, "Durée d’emploi": dureEmp, "jobLocation": jobLocation,
              "move": move[random.randint(0, len(move) - 1)]}
    print(curExp)
    return curExp


def getFromSourcePage(sourcepage, feature):
    soup = BeautifulSoup(sourcepage, 'lxml')
    # soupContent = soup
    EXPS = {}
    cclass = "pvs-list__paged-list-item artdeco-list__item pvs-list__item--line-separated pvs-list__item--one-column"

    list_li = soup.find("main", {"class": "scaffold-layout__main"}).ul.find_all("li",
                                                                                {"class": cclass})  # .text.strip()
    if len(list_li) == 0:
        cclass = "pvs-list__paged-list-item artdeco-list__item pvs-list__item--line-separated"

        list_li = soup.find("main", {"class": "scaffold-layout__main"}).ul.find_all("li",
                                                                                    {"class": cclass})
    exps = "jobTitle,Company name,jobType,Dates d'emploi,Durée d’emploi,jobLocation,Description,Company page,move".split(
        ',')
    # formation = "formationTitle,ecole,Dates d'entrée,Durée de sortie,Description,Company page".split(',')
    educationsRename = {"jobTitle": "Company name", 'Company page': 'Company page', "Company name": "formationTitle",
                        "Dates d'emploi": "Dates d'entrée", "Durée d’emploi": "Durée de sortie",
                        "Description": "Description"}
    EXPS = {k: [] for k in exps}

    def fillExperience(experienceScrap):
        for ex in experienceScrap.keys():
            valueExp = experienceScrap[ex]
            if type(valueExp) != list:
                EXPS.setdefault(ex, []).append(valueExp)
            else:
                EXPS[ex] += valueExp

    for lExp in list_li:
        try:
            expe = getByList(lExp, len(lExp.find("div", {
                "class": "pvs-list__outer-container pvs-entity__sub-components"}).find_all("span", {
                "class": "pvs-entity__path-node"})) > 0)
            if expe is not None:
                fillExperience(expe)
        except:
            try:
                expe = getByList(lExp)
                if expe is not None:
                    fillExperience(expe)
            except Exception as e:
                print("Error....")
                print(str(e))
                pass
    if feature == "education":
        EXPS = {v: EXPS[k] for k, v in educationsRename.items()}
    return EXPS


def getPoepleExpOrSkillV2(driver, feature, linkedinUrl):
    if "/in/ACoA" in linkedinUrl:
        linkedinUrl = linkedinUrl.split("/in/")[0] + "/in/ " + linkedinUrl.split("/in/")[1]
    url = f"{linkedinUrl}/details/{feature}/".replace("//details", '/details')
    driver.get(url)
    time.sleep(random.randint(4, 10))
    sourcepage = driver.page_source
    return getFromSourcePage(sourcepage, feature), sourcepage


onProcess = []
onFailed = []
itUpdated = False
index = 0
link404 = []
pbarGlobal = ""
canScraps = [True for _ in range(20)]