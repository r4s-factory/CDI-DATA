import random
import time
import json
from scrappeFonctions import allLinks
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup
from tqdm import tqdm

def initContenaire(salesDFLink=None):
    print("Reste : ", len(salesDFLink))
    GsalesInfos = {'People ID': [], 'Linkedin ID': [], 'People name': [], 'jobTitle': [], 'People LinkedIn': [],
                   'mediaLink': [], 'summary': [], 'headline': [], 'profilIndustrie': [],
                   'jobLocation': [], 'Formation': [], 'skills': [], "abonneeCount": [], "relation": [],
                   'Company name': [], 'Company ID': [], 'logger': [], 'source': [], 'timestamp': [], "soup": []}

    return GsalesInfos, list(GsalesInfos.keys()), salesDFLink, pd.DataFrame()

def initValues(salesDFLink, salesDFinfo, maj):
    names = []
    compIDS = []
    personNames = []
    personIDS = []
    personLinks = []
    personJobTitle = []
    personJobLocation = []

    f = [names, compIDS, personNames, personIDS, personLinks, personJobTitle, personJobLocation]

    # kkeys = ','.join(salesDFLink.keys()).replace(",jobCorrespondance", '').split(",")
    if len(salesDFinfo) > 1:
        if '' in salesDFLink["People LinkedIn"].values.tolist():
            salesDFLink["People LinkedInRoot"] = "https://www.linkedin.com/in/"
            salesDFLink["People LinkedIn"] = salesDFLink["People ID"].apply(
                lambda x: f"https://www.linkedin.com/in/{x}")
    oldval = []
    kkeys = ['Company name', 'Company ID','People name','People ID','People LinkedIn', 'jobTitle','jobLocation']

    for jk, k in zip(range(len(f)), kkeys):
        #if k in kkeys:
        try:
            val = salesDFinfo[k].loc[salesDFinfo["Linkedin ID"] == ""].values.tolist()
            oldval = val
        except:
            val = ["" for _ in oldval]
        f[jk] = val + salesDFLink[k].values.tolist()
    names, compIDS, personNames, personIDS, personLinks, personJobTitle, personJobLocation = f
    return names, compIDS, personNames, personIDS, personLinks, personJobTitle, personJobLocation, salesDFLink, salesDFinfo


def getImageLink(soup):
    """Get the profil picture of the people"""
    pp = ""
    try:
        try:
            pp = soup.find_all("section")[2].find("img", {
                "class": "pv-top-card-profile-picture__image pv-top-card-profile-picture__image--show ember-view"}).get_attribute_list(
                key="src")[0]
        except:
            pp = soup.find_all("section")[2].find("img").get_attribute_list(key="src")[0]
        if "data:image" in pp.split("/"):
            pp = ""
    except:
        pass
    return pp


def getAbonneNumber(soup):
    """Get his subscriber number"""
    nAbonne = ""
    try:
        nAbonne = soup.find("p", {"class": "pvs-header__subtitle text-body-small"}).text.replace("followers",
                                                                                                 "").replace("abonnés",
                                                                                                             "").replace(
            "Plus de ", "+").strip().split()
        if len(nAbonne) == 2:
            nAbonne = nAbonne[0]
        elif len(nAbonne) == 4:
            nAbonne = ''.join(nAbonne[:2])
        nAbonne = ''.join([l for l in nAbonne if l.isdigit()])
    except:
        pass
    return nAbonne.replace("abonnés", "")


def getRelationCount(soup):
    """Get the relation count"""
    sec = 0
    relation = ""
    try:
        for f in soup.find_all("section"):
            sec += 1
            text = f.text.strip().replace("connections", "relations").replace("Plus de ", "+")
            if "relations" in text:
                lineRelation = text.strip().split("relations")[0].split('\n')[-1]
                lineRelation = ''.join([l for l in lineRelation if l.isdigit() or l == "+"])
                if '+' == lineRelation[0]:
                    lineRelation = lineRelation.replace('+', "")
                    lineRelation = f'{lineRelation}+'
                relation = lineRelation
                return relation
    except:
        pass
    return relation


def getFormation(soupSection):
    section = {"formationTitle": [], "School name": [], "School page": [], "Année debut": [], "Année fin": []}
    for l in soupSection.find_all("li"):
        formTitle, formSchool, schoolStart, schoolEnd, compPage = "", "", "", "", ""
        try:
            if len(l.find_all('span', {"aria-hidden": "true"})) >= 3:
                try:
                    compPage = l.find("a").get_attribute_list(key="href")[0]
                except:
                    pass
                edude = l.find_all('span', {"aria-hidden": "true"})
                formSchool = edude[0].text.strip()
                formTitle = edude[1].text.strip()
                try:
                    schoolStart = edude[2].text.strip().split(" - ")[0]
                    schoolEnd = edude[2].text.strip().split(" - ")[1]
                except:
                    schoolStart = edude[2]
                section.setdefault("formationTitle", []).append(formTitle.strip())
                section.setdefault("School name", []).append(formSchool.strip())
                section.setdefault("School page", []).append(compPage)
                section.setdefault("Année debut", []).append(schoolStart)
                section.setdefault("Année fin", []).append(schoolEnd)
        except:
            pass
    return section


# For Exp
def getExperience(soupSection):
    section = {"jobTitle": [], "Company name": [], "Company page": [], "Date d’emploi": [], "Durée d’emploi": [],
               "jobType": []}
    for l in soupSection.find_all("li"):
        jobTitle, compName, compPage, jobType, datee, duree = "", "", "", "", "", ""

        try:
            try:
                compPage = l.find("a").get_attribute_list(key="href")[0]
            except:
                pass

            l.find("div", {"class": "display-flex flex-column full-width"}).find("div", {
                "class": "display-flex align-items-center mr1 t-bold"})
            # print([s.text for s in l.find_all('span',{"aria-hidden":"true"})[:3]])
            partExp = [s.text.strip() for s in l.find_all('span', {"aria-hidden": "true"})[:3]]
            # Partie experiences
            for st in partExp:
                if " · " in st and " - " in st:
                    # la date et la duré
                    duree = st.split(" · ")[-1]
                    datee = st.split(" · ")[0]
                elif " · " in st:
                    try:
                        compName = st.split(" · ")[0]
                        jobType = st.split(" · ")[-1]
                    except:
                        compName = st
                else:
                    jobTitle = st
            if compName == "":
                jobTitle = partExp[0]
                compName = partExp[1]
                dateDure = partExp[2]
                datee = dateDure.split(" · ")[0]
                duree = dateDure.split(" · ")[-1]
            # print(jobTitle,"---",compName,"----",jobType,"----",datee)

            section.setdefault("jobTitle", []).append(jobTitle.strip())
            section.setdefault("Company name", []).append(compName.strip())
            section.setdefault("Company page", []).append(compPage)
            section.setdefault("jobType", []).append(jobType.strip())
            section.setdefault("Durée d’emploi", []).append(duree)
            section.setdefault("Date d’emploi", []).append(datee)
        except:
            pass

    return section


def getExpForm(sourcepage):
    soup = BeautifulSoup(sourcepage, 'lxml')
    sero = 0
    sects = []
    for c in soup.find_all("section", {"data-view-name": "profile-card"}):
        try:
            myEd = c.find("div", {"class": "pvs-list__outer-container"}).ul
            if myEd != None:
                sects.append(myEd)
                sero += 1

        except:
            pass
        if sero == 2:
            break
    sectExp = getExperience(sects[0])
    sectForm = getFormation(sects[1])
    return sectExp, sectForm


def getCurentExp(soup):
    compName, jobType, startDate, startEnd, duration, compId = "", "", "", "", "", ""
    jobTitle, jobLocation = "", ""
    for sec in soup.find_all("section")[1:]:
        if sec.find("div", {"class": "ph5"}) != None and jobLocation == "":
            try:
                jobLocation = sec.find("div", {"class": "pv-text-details__left-panel mt2"}).text.strip().split("\n")[0]
                jobTitle = sec.find("div", {"class": "text-body-medium break-words"}).text.strip().split("\n")[0]
            except:
                pass
        try:
            if "Expérience" in sec.h2.text:
                pointMillieu = 0
                l = sec.find('ul')
                EXPS = getExp(l, {}, "")
                if jobTitle == "":
                    jobTitle = EXPS["jobTitle"]
                compName = EXPS["Company name"][0]
                compId = EXPS["Company ID"][0]
                content = l.find_all("span", {"class": "visually-hidden"})
                lineExp = 0
                for c in content:
                    if len(c.text) < 50:
                        if " · " in c.text.strip():
                            if pointMillieu == 0:
                                if len(c.text.split(" · ")) == 2:
                                    compName = c.text.split(" · ")[0].strip()
                                    jobType = c.text.split(" · ")[1].strip()
                                else:
                                    compName = c.text.split(" · ")[0].strip()

                            else:
                                startDate = c.text.split(" · ")[0].split("-")[0].strip()
                                startEnd = c.text.split(" · ")[0].split("-")[1].strip()
                                duration = c.text.split(" · ")[1].strip()

                            pointMillieu += 1
                        elif jobLocation == "" or jobTitle == "":
                            if lineExp == 0 and jobTitle == "":
                                jobTitle = c.text.strip()
                            elif jobLocation == "":
                                jobLocation = c.text.strip()
                            lineExp += 1
                break
        except:
            pass
    return [jobTitle, jobLocation, compName, compId]


def getPoepleExpOrSkill(driver, feature, linkedinUrl):
    url = f"{linkedinUrl}/details/{feature}/".replace("//details", '/details')
    driver.get(url)
    time.sleep(random.randint(10, 25))
    sourcepage = driver.page_source
    soup = BeautifulSoup(sourcepage, 'lxml')
    EXPS = {}
    otherF = []
    compName = ""

    def getExp(l, EXPS, compName):
        move = ['↧', '↥', '=']
        exp = "jobTitle,Company name,Dates d'emploi,Durée d’emploi,jobLocation,Description,Company page,move".split(',')
        try:
            exps = l.find_all("span", {"class": "visually-hidden"})
        except:
            return EXPS
        compId = ""

        if not len(exps) < 3:
            usersExp = [s.text.strip() for s, i in zip(exps, range(len(exps))) if
                        i < 5]  # Recupere les 4 premiere informations d'une experience
            try:
                compId = l.find("a").get_attribute_list(key="href")[0]
                compId = compId.split("/")[-2]
                if str(compId).isdigit():
                    compId = f"https://www.linkedin.com/company/{compId}"
                else:
                    compId = ""
            except:
                pass
            if "aujourd’hui" in usersExp[1] or "1an " in usersExp[1] or "ans " in usersExp[1] or "mois" in usersExp[
                1] or "yrs" in usersExp[1] or "mos" in usersExp[1]:
                if compName == "":
                    compName = usersExp[0]
                    del usersExp[0]
                    del usersExp[0]

            if len(usersExp) < 5:  # S'il manque des informations comme la localisation par exemple
                for i in range(5 - len(usersExp)):
                    # usersExp.insert(3+i,"")
                    usersExp.append("")
            d = True
            ddate = False
            locat = 0

            for ji in range(len(usersExp) - 1):
                if ji == 0:
                    EXPS.setdefault(exp[0], []).append(usersExp[0])
                if ji == 1:
                    if compName == "":
                        if " · " in usersExp[ji]:
                            usersExp[ji] = usersExp[ji].split(" · ")[0]
                        EXPS.setdefault(exp[ji], []).append(usersExp[ji])
                    else:
                        EXPS.setdefault(exp[1], []).append(compName)
                if " an" in usersExp[ji] or " ans" in usersExp[ji] or " mois" in usersExp[ji] or " yrs" in usersExp[
                    ji] or " mos" in usersExp[ji]:
                    if " · " in usersExp[ji]:
                        b = usersExp[ji].split(" · ")
                        EXPS.setdefault("Dates d'emploi", []).append(b[0].replace("yrs", "ans").replace("mos", "mois"))
                        EXPS.setdefault('Durée d’emploi', []).append(b[1])
                    else:
                        EXPS.setdefault("Dates d'emploi", []).append(
                            usersExp[ji].replace("yrs", "ans").replace("mos", "mois"))
                        EXPS.setdefault('Durée d’emploi', []).append("")
                    ddate = True
                    locat = ji + 1
                if ddate:
                    ddate = False
                    if "\n" in usersExp[ji + 1] or len(usersExp[ji + 1].split()) > 15:
                        EXPS.setdefault("Description", []).append(usersExp[locat])
                        EXPS.setdefault("jobLocation", []).append("")
                        d = False
                    else:
                        EXPS.setdefault("jobLocation", []).append(usersExp[locat])
            if d:
                EXPS.setdefault("Description", []).append(usersExp[-1])
        EXPS.setdefault("Company page", []).append(compId)
        EXPS.setdefault(exp[-1], []).append(move[random.randint(0, len(move) - 1)])
        return EXPS

    try:
        for l in soup.find("main", {"class": "scaffold-layout__main"}).ul.find_all("li"):
            if feature != "experience":
                try:
                    otherF.append(l.find("span", {"class": "visually-hidden"}).text.strip())
                except:
                    pass
            else:
                try:
                    ids = l.get_attribute_list('id')[0]
                    if "profilePagedListComponent" in ids:
                        if not "profilePositionGroup" in ids:
                            compName = l.find_all("span", {"class": "visually-hidden"})[0].text
                            EXPS = getExp(l, EXPS, "")
                        else:
                            EXPS = getExp(l, EXPS, compName)
                except:
                    pass
    except:
        pass

    time.sleep(random.randint(15, 30))
    if feature != "experience":
        try:
            return ','.join(otherF)
        except:
            return ''
    return EXPS


def cleanAnneDebut(v):
    if type(v) != str:
        v = v.text.strip()
    if ("20" not in v and 'Grade' not in v) or (len(v) > 15):
        v = ""
    return v


def fillSalesInfo(line):
    soup = BeautifulSoup(line["soup"], parser="lxml")

    def fillExpForm(index, oneExp):
        for k, v in oneExp.items():
            if index == 1:
                k = k.replace("Company", "School")
            lk = [kl for kl in allLinks.getExp()[index] if k in kl]
            for ij in enumerate(lk):
                i = ij[0]
                title = ij[1]
                if index == 1:
                    title = title.replace("Company", "School").replace("Durée", "Dates")
                try:
                    if "Année debut_" in title or "Année fin_" in title:
                        v[i] = cleanAnneDebut(v[i])
                    line[title] = v[i]
                except:
                    line[title] = ''
                if i == 3:
                    break

    if str(line["Linkedin ID"]) == "-1" or str(line["Linkedin ID"]) == "":
        try:
            line["Linkedin ID"] = \
                json.loads(soup.find_all("code")[0].getText().strip())["data"]["elements"][0]["lixTracking"][
                    "urn"].split(
                    ":")[-1]
        except:
            pass
    if str(line["summary"]) == "" or str(line["summary"]) == "New format":
        try:
            line["summary"] = getHeadLine(soup).text.strip()
        except:
            pass
    exp, form = getExpForm(line["soup"])
    fillExpForm(0, exp)
    fillExpForm(1, form)
    return line


def getHeadLine(headline):
    try:
        main = headline.find("section", {"class": "artdeco-card pv-profile-card break-words mt2"}).find("div", {
            "class": "display-flex ph5 pv3"}).span
        return main.getText()
    except:
        return "Erreur Scrappe"


def ingestionPeopleData(logger, driver, sourcepage):
    """Fonction qui permettra de recupérer les infos sur la page linkedin et de les telecharger via beautiful soup"""
    salesInfo = {'People name': [], 'jobTitle': [], 'jobType': [], 'jobLocation': [], 'Linkedin ID': [],
                 'People LinkedIn': [], 'mediaLink': [], 'Formation': [], 'Experience': [],
                 'skills': [], "abonneeCount": [], "relation": [],
                 'Company name': [], 'Company ID': [], "GoodLink": [], 'logger': [],
                 'timestamp': [], "soup": []}  # "Dates d'emploi": [], "Durée d’emploi": [],
    feature = ['experience', "education", 'skills']

    dataMemberId = ""
    linkedinUrl = driver.current_url
    soup = BeautifulSoup(sourcepage, 'lxml')
    try:
        main = soup.find("main", {"id": "main"})
        sections = main.find_all("section")
        dataMemberId = sections[0].get_attribute_list(key="data-member-id")[0]
    except:
        time.sleep(random.randint(5, 10))
        try:
            main = soup.find("main", {"id": "main"})
            sections = main.find_all("section")
            dataMemberId = sections[0].get_attribute_list(key="data-member-id")[0]
        except:
            try:
                dataMemberId = json.loads(soup.find_all("code")[0].getText().strip())["data"]["elements"][0]["lixTracking"][
                        "urn"].split(":")[-1]
            except:
                print("Have not scrappe dataMemberId by : ", logger)
    salesInfo.setdefault("Linkedin ID", []).append(dataMemberId)
    salesInfo.setdefault("People LinkedIn", []).append(linkedinUrl)
    jobTitle, jobLocation, compName, compId = getCurentExp(soup)
    salesInfo.setdefault("jobTitle", []).append(jobTitle)
    # salesInfo.setdefault("jobType", []).append(jobType)
    salesInfo.setdefault("Company name", []).append(compName)
    salesInfo.setdefault("Company ID", []).append(compId)
    salesInfo.setdefault("jobLocation", []).append(jobLocation)
    # salesInfo.setdefault("Dates d'emploi", []).append(f"{startDate} - {startEnd}")
    # salesInfo.setdefault("Durée d’emploi", []).append(duration)

    salesInfo.setdefault("mediaLink", []).append(getImageLink(soup))

    salesInfo.setdefault("relation", []).append(getRelationCount(soup))
    salesInfo.setdefault("abonneeCount", []).append(getAbonneNumber(soup))

    salesInfo.setdefault("skills", []).append(
        getPoepleExpOrSkill(driver=driver, feature=feature[2], linkedinUrl=linkedinUrl))
    # salesInfo.setdefault("skills", []).append("New Format")

    # salesInfo.setdefault("Formation", []).append("New Format")
    salesInfo.setdefault("Formation", []).append(
        getPoepleExpOrSkill(driver=driver, feature=feature[1], linkedinUrl=linkedinUrl))
    salesInfo.setdefault('logger', []).append(logger)
    salesInfo.setdefault('timestamp', []).append(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    salesInfo.setdefault("summary", []).append(getHeadLine(soup))
    salesInfo.setdefault("headline", []).append("New format")
    salesInfo.setdefault("profilIndustrie", []).append("New format")
    print(salesInfo)
    salesInfo.setdefault("soup", []).append(soup)

    return salesInfo
