from bs4 import BeautifulSoup

file_name = "EDM vs. FLA (2024-06-24) ShiftChart.com.mhtml"

with open(file_name, 'r', encoding='utf-8') as file:
    html_content = file.read()

soup = BeautifulSoup(html_content, 'html.parser') 

all_g = soup.find_all('g')


playerRows = soup.find_all('g', class_='3D"playerRow')

allShifts = soup.find_all('g', class_='3D"gAllShifts"')
#print(allShifts)
allPlayers = soup.find_all('g', class_='3D"playerRow H"')
#for player in allPlayers:
    #print(player)
#for player in allPlayers:
    #playerName = player.find_all('text')
    #print(playerName)
    #print("\n")
    #innerHTML= playerName[0].decode_contents()
    #print(innerHTML)
    #print(playerName)

for player in playerRows:
    #print(player)
    #print("\n")
    allShiftp = player.find_all('g',class_='3D"gAllShifts"')
    #print(allShiftp)
    for children in player.findAll():
        print(children.attrs)

    for allshift in allShiftp:
        gs = allshift.find_all('g')
        gshifts = allshift.find_all('g', class_='3D"gShift"')
        for g in gshifts:
            #print(g)
            gRec = g.find_all('rect')
            #print(gRec)
            for rec in gRec:
                lol = True
                #print(rec)
                #print("\n")
                #output = rec[0]['x']
                #print(rec.attrs['width'])
                #print("\n")
    #print(allShiftp)
    