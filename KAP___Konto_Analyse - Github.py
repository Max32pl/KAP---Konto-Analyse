
from multiprocessing.connection import Connection
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.keys import Keys
import time
from datetime import datetime
import os
import glob
import shutil
import mysql.connector as mc
import sys
import logging


def download_kontoauszüge():

    try:

        #Login Daten 
        username = '*******'
        password = '*******'

        #Webdriver und Website öffnen 
        driver = webdriver.Chrome(r"Path\chromedriver.exe")
        #driver.minimize_window()
        driver.maximize_window()
        driver.get('Path/Website')

        time.sleep(2)

        #Loginfeld für Nutzername bzw. Passwort - Eingabe Nutzername bzw. Passwort
        element_username = driver.find_element(By.XPATH, '/html/body/div/header/div/div[2]/form/input[1]')
        element_username.send_keys(username)

        element_password = driver.find_element(By.XPATH, '/html/body/div/header/div/div[2]/form/input[2]')
        element_password.send_keys(password)

        #Login Button klicken
        driver.find_element(By.XPATH, '//input[@type="submit"]').click()

        time.sleep(3)

        #Umsätze klicken
        driver.find_element(By.XPATH, '/html/body/div/section/div/div/div[3]/form/div[2]/table/tbody/tr[2]/td[5]/div[1]/input').click()

        time.sleep(3)

        #Exportieren klicken
        driver.find_element(By.XPATH, '/html/body/div/section/div/div/div[4]/form/div[4]/div/div[2]/ul/li[4]/div/a[1]').click()

        time.sleep(3)

        #Exportieren klicken
        driver.find_element(By.XPATH, '/html/body/div/section/div/div/div[4]/form/div[4]/div/div[2]/ul/li[4]/ul/li[1]/div/input').click()

        time.sleep(20)

        driver.close()

        log.info("Auslesen der Website und Download erfolgreich")

    except:

        log.warning('Fehler beim auslesen des Kontoauszugs / Webscraping fehlgeschlagen')

def laden_kontoauszüge():

    try:

        #Speicherort der Kontoauszüge
        path = r'Path\*.CSV'

        #Auslesen der im Ordner enthaltenden Files & neuster File
        list_of_files = glob.glob(path)
        latest_file = max(list_of_files, key=os.path.getctime)
        #print('Files:', list_of_files)
        #print('Latest Files:', latest_file)

        #Neusten Kontoauszug in Dataframe laden, Dataframe auf relevante Columns beschränken, NaN-Einträge ersetzen
        df_neuer_kontoauszug = pd.read_csv(latest_file, sep=";")
        df_neuer_kontoauszug = df_neuer_kontoauszug.drop(columns=['Valutadatum','Sammlerreferenz','Lastschrift Ursprungsbetrag'
                                                                  ,'Auslagenersatz Ruecklastschrift'], axis=1)
        df_neuer_kontoauszug = df_neuer_kontoauszug.fillna('')
        with pd.option_context('display.max_rows', 10, 'display.max_columns', None):
            print(df_neuer_kontoauszug)

        #Verschieben des Files an neuen Speicherort
        path_neu = r'Path\Kontoauszüge'
        shutil.move(latest_file, path_neu)

        log.info('Auslesen und Verschieben des Kontoauszugs erfolgreich')

        return df_neuer_kontoauszug

    except:

        log.warning('Fehler beim Einlesen eines Kontoauszugs / Keine Datei im Ordner')

        var = 'empty'

        return var

def daten_in_database(df_neuer_kontoauszug):

    try: 

        #Verbindung zur Datenbank herstellen
        my_database = mc.connect (host = '*******',
                                 user = '******',
                                 passwd = '*******',
                                 db = '******')

        print(my_database)

        log.info('Verbindung zur Datenbank hergestellt')
    
    except:
       
        log.error(mc.Error)
    
    try: 
        
        #Letzten Eintrag der Datenbank auslesen - SQL to Dataframe
        sql_befehl ="""
                        SELECT * FROM kontoauszüge
                        ORDER BY id DESC
                        LIMIT 1
                    """

        sql_query = pd.read_sql_query(sql_befehl, my_database)

        df_database_lastline = pd.DataFrame(sql_query)
        
        with pd.option_context('display.max_rows', 2, 'display.max_columns', None):
            print(df_database_lastline)

        log.info('Auslesen des letzten Datenbankeintrags erfolgreich')

    except:

        log.error('Fehler beim Auslesen der Datenbank - Tabelle kontoauszüge')

    try:

        #Letzte Datenreihe der Datenbank im Kontoauszug suchen - Deklarieren des ersten neuen Eintrags
        for i in df_neuer_kontoauszug.index: 
        
            #print('Index Kontoauszug: ', i)

            if df_neuer_kontoauszug.loc[i, 'Kontonummer/IBAN'] == df_database_lastline.loc[0, 'iban'] and df_neuer_kontoauszug.loc[i, 'Verwendungszweck'] == df_database_lastline.loc[0, 'verwendungszweck']:

                index_neuer_eintrag = i+1
    
        print('Index für ersten neuen Datensatz: ', index_neuer_eintrag)

        #Eintragen der neuen Kontobewegungen in Datenbank
        db_cursor = my_database.cursor()

        for i in range(index_neuer_eintrag, len(df_neuer_kontoauszug), 1):

            val = (df_neuer_kontoauszug.loc[i, 'Auftragskonto'], df_neuer_kontoauszug.loc[i, 'Buchungstag'], df_neuer_kontoauszug.loc[i, 'Buchungstext']
                   , df_neuer_kontoauszug.loc[i, 'Verwendungszweck'], df_neuer_kontoauszug.loc[i, 'Glaeubiger ID']
                   , df_neuer_kontoauszug.loc[i, 'Mandatsreferenz'], df_neuer_kontoauszug.loc[i, 'Kundenreferenz (End-to-End)']
                   , df_neuer_kontoauszug.loc[i, 'Beguenstigter/Zahlungspflichtiger'], df_neuer_kontoauszug.loc[i, 'Kontonummer/IBAN']
                   , df_neuer_kontoauszug.loc[i, 'BIC (SWIFT-Code)'], df_neuer_kontoauszug.loc[i, 'Betrag'], df_neuer_kontoauszug.loc[i, 'Waehrung']
                   , df_neuer_kontoauszug.loc[i, 'Info'])

            sql_befehl ="""
                            INSERT INTO kontoauszüge 
                            (auftragskonto, buchungstag, buchungstext, verwendungszweck, gläubiger_id
                            , mandatsreferenz, kundenreferenz, beguenstigter, iban, bic, betrag
                            , währung, info) 
                            VALUE (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """
        
            db_cursor.execute(sql_befehl, val)

            my_database.commit()

        log.info('Eintragen der neuen Kontobewegungen erfolgreich')

    except:

        log.error('Fehler beim Eintragen von neuen Daten in die Datenbank')


if __name__ == "__main__":

    """Automatisiertes Verwalten und Analysieren von Kontobewegungen"""

    #Logdatei 
    Log_Format = "%(levelname)s %(asctime)s - %(message)s"
    logging.basicConfig(filename = r'Path\logfile.log',
                    filemode = "a",
                    format = Log_Format, 
                    level = logging.DEBUG)
    log = logging.getLogger()

    """1. Login und Download von Kontoauszügen von der Website der Bank"""
    download_kontoauszüge()

    """2. Laden der Kontoauszüge - .CSV to Dataframe"""
    df_neuer_kontoauszug = laden_kontoauszüge()

    """3. Filtern und Abspeichern der neuen Kontobewegungen in Datenbank"""
    if  isinstance(df_neuer_kontoauszug, str) == True: 
        log.warning('Keine .CSV-Datei gefunden - Kein Eintrag in Datenbank')
    else:
        daten_in_database(df_neuer_kontoauszug)

    
