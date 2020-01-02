# PTV SISTeMA 2019 -- Authors: GG,MB,GC

#from scipy.optimize import curve_fit
# from scipy.optimize import curve_fit
import math
from time import strftime
import os

import matplotlib.pyplot as plt
import numpy as np
import psycopg2



print("")
print("/////////////////////////////////////////////////////////////////////////////////////////////////")
print("This Python script aims to Collect DETECTED traffic data in OPTIMA and make an evaluation of KPI.")
print("/////////////////////////////////////////////////////////////////////////////////////////////////")
print("")


#######################################################################DATABASE SECTION

postgres_server = 'localhost'
postgres_port = 5432
postgres_user = 'postgres'
postgres_password = 'postgres'
postgres_db = 'otpima_socrates'

######################################################################


conn = psycopg2.extensions.connection
cursor = psycopg2.extensions.cursor

conn = psycopg2.connect(host='localhost',
                        port=5432,
                        database='optima',
                        user='postgres',
                        password='postgres')
cursor = conn.cursor()

print("Connection to OPTIMA database established \n")

#####################################################################
# Section TRE parameters
#####################################################################
NumSimuIni=63394 #first 'simu' which the script starts from
NumSimu=90 #Number of simulations that script must iterate
delta=5 #delta (width of TRE simulation interval)
ifflow = 1 #boolean: 0 not calculate KPIs for flow, 1 yes
ifsped = 0 #boolean: 0 not calculate KPIs for speed, 1 yes

######################################################################
# Section KPI parameters
#####################################################################
forecastDistance=[10,15] #forecast distance in minutes 
thresholdSPED = 0.25 # for defining how many links have the KPIs under the treshold
thresholdFlowList = [12,14] # for defining how many links have the KPI under the threshold

######################################################################
# Section Script parameters
#####################################################################
ifDebug=0 # 0 = no Debug // 1 = only info about Network Elements exposed // 2 = full debug
ifPlottingGraph = 1 # 0 = NO result plots // 1 = YES result plots
ifCreateDedicatedFolder = 1 # 0 = NO dedicated folder are created // 1 = YES dedicated folder are created

######################################################################

if ifDebug == 0:
    print('No Debug Active \n')
elif ifDebug == 1:
    print('Network Elements Count Active \n')
elif ifDebug > 2:
    print('Full Debug Active \n')
        
######################################################################
iter=0

for fd in forecastDistance:
    thresholdFLOW = thresholdFlowList[iter]
    ######################################################################
    if ifCreateDedicatedFolder == 0:
        save_path_plot = 'C:/Users/Giacomo.Cavalleri/Documents/GitHub/KPI_output/' #select the folder you want to store the plots
        print('No Dedicated Folder\n')
    else:   
        if ifflow > 0:
            whichKPI = 'KPI_Flow_t' + str(fd)
        else:
            whichKPI = 'KPI_Speed_t' + str(fd)
        
        save_path = 'C:/Users/Giacomo.Cavalleri/Documents/GitHub/KPI_output' #select the folder you want to store the plots
        save_path_plot = 'C:/Users/Giacomo.Cavalleri/Documents/GitHub/KPI_output/' + whichKPI + '/' #select the folder you want to store the plots
        os.mkdir(save_path_plot)
        print('Dedicated Folder Created \n')
    fileNameCSV = save_path_plot + 'kpi_values.csv' #select the filename of CSV file
    print(fileNameCSV)
    
    kpi = []
    kpi_ind = []
    perc1_kpi = []
    perc2_kpi = []
    f = open(fileNameCSV , "w+")
    header = 'Average KPI      , Time , % Above Threshold \n'
    f.write("%s" % header)
    ######################################################################
    for IndxSimu in range (0, NumSimu): #for IndxSimu in range (1, NumSimu)

        Simu = NumSimuIni + IndxSimu  # Simu=NumSimuIni+IndxSimu
        print('KPI t %s, iteration %s over %s -- Simu Num. %s \n' %(fd,(IndxSimu+1),NumSimu,Simu))
        
        Strt = []
        Fsnd = []
        Detected = []
        Simulated = []
        Inst = []
        Endtime = []

        if ifsped > 0:

            query = """select out1.strt,out1.fsnd,out1.tsped,out1.rsped, out2.inst, out2.run_start_inst from ( Select t.strt,t.fsnd,t.sped as tsped,r.sped as rsped,r.simu,r.endtime,t.ldat from tsta t join rlin_tsys_tre r on (r.link, r.fnod) = (t.strt, t.fsnd) where t.tsys is null AND r.simu=%s AND t.ldat =  r.endtime and t.sped > 0) as out1 join (select s.idno,s.run_start_inst,s.simulated_from,s.simulated_to, substring(cast(s.inst as varchar),9,4) as inst from simu as s )as out2 on (out1.simu)=(out2.idno) where out2.run_start_inst > out1.endtime - '%s min'::interval and out2.run_start_inst<=out1.endtime-'%s min'::interval+'%s min'::interval""" % (Simu, fd, fd, delta)

        #        Path=11
        #        query = """select out1.strt,out1.fsnd,out1.tsped,out1.rsped,out1.inst from ( Select t.strt,t.fsnd,t.sped as tsped,r.sped as rsped,r.simu,r.inst,r.fore,r.starttime,r.endtime,t.cdat,t.fdat,t.ldat from %s t join rlin_tsys_tre r on (r.link, r.fnod) = (t.strt, t.fsnd) where (Link, fnod) in (select distinct link, "fromNode" from path_resu_link where path=%s ) and t.tsys is null and t.fdat+'17 min'::interval >=  r.starttime AND t.ldat <=  r.endtime and t.sped > 0 order by t.strt ) as out1 join (select s.idno,s.run_start_inst,s.simulated_from,s.simulated_to from simu as s ) as out2 on (out1.simu)=(out2.idno) where out2.simulated_from = out1.fdat+'17 min'::interval-'%s min'::interval""" % (tstatable,Path, shiftRlin)

        else:
            fakeelse = 1
        if ifflow > 0:
            query = """select out1.strt,out1.fsnd,out1.tflow,out1.rflow, out2.inst, out2.run_start_inst  from ( Select t.strt,t.fsnd,t.flow as tflow,r.oflw as rflow,r.simu,r.endtime,t.ldat from tsta t join rlin_tsys_tre r on (r.link, r.fnod) = (t.strt, t.fsnd) where t.tsys is null AND r.simu=%s AND t.ldat =  r.endtime and t.flow > 0) as out1 join (select s.idno,s.run_start_inst,s.simulated_from,s.simulated_to, substring(cast(s.inst as varchar),9,4) as inst from simu as s )as out2 on (out1.simu)=(out2.idno) where out2.run_start_inst > out1.endtime - '%s min'::interval and out2.run_start_inst<=out1.endtime-'%s min'::interval+'%s min'::interval""" % (Simu, fd, fd, delta)

            #"""select out1.strt,out1.fsnd,out1.tflow,out1.rflow,concat(extract(hour from out2.insta),extract(minute from out2.insta)) from ( Select t.strt,t.fsnd,t.flow as tflow,r.oflw as rflow,r.simu,r.inst,r.fore,r.starttime,r.endtime,t.cdat,t.fdat,t.ldat from tsta t join rlin_tsys_tre r on (r.link, r.fnod) = (t.strt, t.fsnd) where r.link=5839829 and r.fnod=2000006632 and t.tsys is null and t.flow is not null and t.fdat =  r.starttime + '10 min'::interval AND r.simu=%s AND t.ldat =  r.endtime and t.sped > 0 order by t.strt) as out1 join (select s.idno,s.run_start_inst as insta,s.simulated_from,s.simulated_to from simu as s ) as out2 on (out1.simu)=(out2.idno) where out2.simulated_from = out1.fdat-'%s min'::interval""" % (Simu, shiftRlin)
        else:
            fakeelse = 1

        
        cursor.execute(query)
        outvarnet = cursor.fetchall()
        if ifDebug == 2:
            print(query)
            print("")
            print(outvarnet)

        ii = 0
        for variable in outvarnet:
            Strt.append(variable[0])
            Fsnd.append(variable[1])
            Detected.append(variable[2])
            Simulated.append(variable[3])
            Inst.append(variable[4])
            Endtime.append(variable[5])

            ii = ii + 1
        if ifDebug > 0:
            print("")
            print("List of unique INST identifiers")
            print(set(Inst))
            print("")
            print("Number Streets")
            print(len(Strt))
            print("Number Fsnd")
            print(len(Fsnd))
            print("Number Detec")
            print(len(Detected))
            print("Number Simu")
            print(len(Simulated))
            print("Number Inst")
            print(len(Inst))

        if ifsped > 0:

            kpisum = 0
            percentual1 = 0
            percentual2 = 0
            percentual3 = 0
            for Indx in range(0, len(Detected)):
                #            print(Simulated[Indx],Detected[Indx],Simulated[Indx]-Detected[Indx],abs(Simulated[Indx]-Detected[Indx]))
                kpisum = kpisum + (abs(Simulated[Indx] - Detected[Indx])/Detected[Indx])


                if ((abs(Simulated[Indx] - Detected[Indx])/Detected[Indx]) <= thresholdSPED ):
                    percentual1 = percentual1 + 1

                else:
                    fake1ifstat = 1

        else:
            fakeelse = 1

        if ifflow > 0:

            kpisum = 0
            percentual1 = 0
            percentual2 = 0
            percentual3 = 0
            for Indx in range(0, len(Detected)):
                kpisum = kpisum + math.sqrt(2 * (Simulated[Indx] - Detected[Indx]) * (Simulated[Indx] - Detected[Indx]) / (
                            Simulated[Indx] + Detected[Indx]))

                if (math.sqrt(
                        2 * (Simulated[Indx] - Detected[Indx]) ** 2 / (Simulated[Indx] + Detected[Indx])) < thresholdFLOW):
                    percentual1 = percentual1 + 1

                else:
                    fake1ifstat = 1

        else:
            fakeelse = 1

        if len(Strt) > 0:
            kpisum = kpisum / len(Strt)
            percentual1 = percentual1 * 100 / len(Strt)
        else:
            kpisum = 0
            percentual1 = 0

        kpi.append(kpisum)
        kpi_ind.append(Inst[0])
        perc1_kpi.append(percentual1)
        endt= strftime("%H:%M", Endtime[0].timetuple())
        AA = str(kpisum) + ', ' + endt[-8:] + ', ' + str(percentual1) + ' \n'
        f.write("%s" % AA)
        print("KPI values")
        print("==========")
        print("Time %s " %Inst[0])
        print('Average KPI %s'  % kpisum)
        print('Percentage below threshold %s \n' % percentual1)

    if ifPlottingGraph == 1:
        if ifsped>0 :
            print("")
            print("DATA Plots (Start).....")
            fig = plt.figure('KPI_NET_SPEED')
            plot_title = 'KPI_SPEED'
            plt.title(plot_title)
            plt.plot(kpi_ind, kpi, '-ok', color='black')
            plt.ylim(0, 1)
            plt.xticks(rotation=90)
            resultsfileplot = 'KPI_NET_SPEED'
            completeName = os.path.join(save_path_plot, resultsfileplot)
            print(completeName)
            fig.savefig(completeName)
            print("DATA Plots (End).....")


            print("")
            print("DATA Plots (Start).....")
            fig = plt.figure('PERC_NET')
            plot_title = 'PERC'
            plt.title(plot_title)
            plt.plot(kpi_ind, perc1_kpi, '-ok', color='black')
            plt.yticks(np.arange(0, 100, step=10))
            plt.xticks(rotation=90) #[df_tags['week'].min()
            resultsfileplot = 'PERC_NET'
            completeName = os.path.join(save_path_plot, resultsfileplot)
            print(completeName)
            fig.savefig(completeName)
            print("DATA Plots (End).....")


        if ifflow > 0 :
            print("")
            print("DATA Plots (Start).....")
            fig = plt.figure('KPI_NET_FLOW')
            plot_title = 'KPI_FLOW'
            plt.title(plot_title)
            plt.plot(kpi_ind, kpi, 'bo--')
            plt.ylim(0, 20)
            plt.xticks(rotation=90)
            resultsfileplot = 'KPI_NET_FLOW'
            completeName = os.path.join(save_path_plot, resultsfileplot)
            print(completeName)
            fig.savefig(completeName)
            print("DATA Plots (End).....")

            print("")
            print("DATA Plots (Start).....")
            fig = plt.figure('PERC_NET')
            plot_title = 'PERC'
            plt.title(plot_title)
            plt.plot(kpi_ind, perc1_kpi, 'ro--')
            plt.yticks(np.arange(0, 100, step=10))
            plt.xticks(rotation=90) #[df_tags['week'].min()
            resultsfileplot = 'PERC_NET'
            completeName = os.path.join(save_path_plot, resultsfileplot)
            print(completeName)
            fig.savefig(completeName)
            print("DATA Plots (End).....")
    iter+=1
conn.close()
print("")
print("Connection to OPTIMA database closed")
