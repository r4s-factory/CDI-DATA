def getExp():
    salesExp = {'People name': [], 'People ID': [], 'People LinkedIn': [], 'logger': [], 'timestamp': [],
                'Donnee Brute': []}
    salesFormation = salesExp.copy()  # Pourquoi tu as changer bordel de merde?????????????
    formation = ['School name', 'School page', 'School ID', 'formationTitle', 'Grade', 'Etude', "Année debut",
                 'Année fin', 'Description']

    exp = "jobTitle,Company name,jobType,Dates d'emploi,Durée d’emploi,jobLocation,Description,Company page,move".split(
        ',')
    for i in range(10):
        for e in exp:
            salesExp[f"{e}_{i + 1}"] = []
        for f in formation:
            salesFormation[f"{f}_{i + 1}"] = []
    return list(salesExp.keys()), list(salesFormation.keys())


