import psycopg2
import os
import matplotlib.pyplot as plt
import numpy as np
from scipy.optimize import curve_fit
import math

print("")
print("/////////////////////////////////////////////////////////////////////////////////////////////////")
print("This Python script aims to Collect DETECTED traffic data in OPTIMA and make an evaluation of KPI.")
print("/////////////////////////////////////////////////////////////////////////////////////////////////")
print("")

postgres_server = '10.3.9.7'
postgres_port = 5432
postgres_user = 'postgres'
postgres_password = 'postgres'
postgres_db = 'optima'

conn = psycopg2.extensions.connection
cursor = psycopg2.extensions.cursor

conn = psycopg2.connect(host=postgres_server,
                        port=postgres_port,
                        database=postgres_db,
                        user=postgres_user,
                        password=postgres_password)
cursor = conn.cursor()

print("Connection to OPTIMA database established")

NumSimuTstaIni=2
NumSimuTsta=4 			#MIN VALUE = 2
SimuRlinStep=2 			#MIN VALUE = 2
threshold1 = 10.0  		#threshold of the absolute deviation --> |OBS-SIMU|<threshold1
threshold2 = 10
threshold4 = 0.20  		#threshold of the relative deviation |**|PERCENTAGE|**|--> (|OBS-SIMU|)/OBS<threshold4
shiftRlin = -1 			#-1 -3-0 mins forecast 0 means 0-3min forecast; 1 means 3-6mins forecast


ifflow = 0
ifsped = 1
ifspedselected = 1

sped_low = 0
sped_hi = 50

save_path = 'C:\Users\ptvuser\KPI'

kpi = []
kpi_ind = []
perc1_kpi = []
perc2_kpi = []

f = open("kpi_verbose.txt","w+")

for IndxTstaSim in range(1, NumSimuTsta):

    SimuTsta=NumSimuTstaIni+IndxTstaSim

    print("")
    print(SimuTsta)

    Strt = []
    Fsnd = []
    Detected = []
    Simulated = []
    Inst = []
    NumVal = []

    for IndxRlinSim in range(1, SimuRlinStep):

            SimuRlin= SimuTsta - IndxRlinSim - shiftRlin

            if SimuRlin > 0:

                        print("")
                        print("============")
                        print("Tsta Simu %d" % (SimuTsta))
                        print("Rlin Simu %d" % (SimuRlin))
                        print("============")
                        print("")

                        tstatable="tsta_30rkpi_bk_{0}".format(SimuTsta)
                        rlintable="rlin_tsys_tre_30bk_{0}".format(SimuRlin)

                        print(tstatable)
                        print(rlintable)
                        print("")
                        if ifsped > 0 :
                            if ifspedselected >0:
                                query = """Select t.strt,t.fsnd,t.sped,r.sped,r.inst from %s t join %s r on (link, fnod) = (strt, fsnd) where t.tsys is null and t.fdat+'17 min'::interval >=  r.starttime AND t.ldat <=  r.endtime and t.sped > %s and t.sped < %s order by t.strt ;""" % (tstatable, rlintable, sped_low, sped_hi)
                            else:
                                query = """Select t.strt,t.fsnd,t.sped,r.sped,r.inst from %s t join %s r on (link, fnod) = (strt, fsnd) where t.tsys is null and t.fdat+'17 min'::interval >=  r.starttime AND t.ldat <=  r.endtime and t.sped > 0 order by t.strt ;""" % (tstatable, rlintable)
                        else:
                            fakeelse=1

                        if ifflow > 0 :
                            query = """Select t.strt,t.fsnd,t.flow,r.oflw,r.inst from %s t join %s r on (link, fnod) = (strt, fsnd) where t.tsys is null and t.fdat+'17 min'::interval >=  r.starttime AND t.ldat <=  r.endtime and t.flow > 0 order by t.strt ;""" % (tstatable, rlintable)
                        else:
                            fakeelse=1

                        print(query)
                        print("")
                        cursor.execute(query)
                        outvarnet = cursor.fetchall()
                        print("")
                        #print(outvarnet)

                        ii=0
                        for variable in outvarnet:

                                if ( variable[0] in Strt and variable[1] in Fsnd
                                ):

                                    # print(Detected[ii],variable[2])
                                    Detected[ii]=Detected[ii]+variable[2]
                                    # print(Detected[ii],variable[2])

                                    # print(Simulated[ii],variable[3])
                                    Simulated[ii]=Simulated[ii]+variable[3]
                                    # print(Simulated[ii],variable[3])

                                    # print(NumVal[ii])
                                    NumVal[ii]=NumVal[ii]+1
                                    # print(NumVal[ii])

                                else:


                                    Strt.append(variable[0])
                                    # print(Strt[ii],variable[0])
                                    Fsnd.append(variable[1])
                                    # print(Fsnd[ii],variable[1])
                                    Detected.append(variable[2])
                                    #  print(Detected[ii],variable[2])
                                    Simulated.append(variable[3])
                                    #  print(Simulated[ii],variable[3])
                                    Inst.append(variable[4])
                                    #  print(Inst[ii],variable[4])
                                    NumVal.append(1)
                                    #  print(NumVal[ii])

                                ii=ii+1

            else:

                        print("No enough RLIN TABLE back in list of SIMU")

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
    print("Number Ele")
    print(len(NumVal))

    for Indx in range(0, len(NumVal)):
        Detected[Indx]=Detected[Indx]/NumVal[Indx]
        Simulated[Indx]=Simulated[Indx]/NumVal[Indx]

    if ifsped > 0 :

        kpisum = 0
        percentual1=0.0			# percentage of links with kpi value lower than threshold1 (absolute deviation)
        percentual2=0.0
        percentual3=0.0
        percentual4=0.0			# percentage of links with kpi value lower than threshold4 (relative deviation)
        percentual5=0.0			# percentage of links with kpi value lower than threshold1 AND threshold4 (absolute and relative deviation)

        for Indx in range(0, len(Detected)):
            kpisum= kpisum + abs(Simulated[Indx]-Detected[Indx]) # / Detected[Indx]

            # print(abs(Simulated[Indx]-Detected[Indx]))

            if ( Detected[Indx] < threshold2 ):
                percentual3=percentual3+1
            else:
                fake3ifstat=1

            if (abs(Simulated[Indx]-Detected[Indx]) < threshold1 ):
                # print("debug")
                percentual1=percentual1+1
                if ((abs(Simulated[Indx]-Detected[Indx])/Simulated[Indx]) < threshold4):
                    percentual5=percentual5+1

                if(Detected[Indx] < threshold2 ):
                       percentual2=percentual2+1
                else:
                    fake2ifstat=1

            else:
                fake1ifstat=1

            if ((abs(Simulated[Indx]-Detected[Indx])/Simulated[Indx]) < threshold4):
                
                percentual4=percentual4+1

            else:
                fake1ifstat=1
        # print(type(abs(Simulated[Indx]-Detected[Indx])))
        # print(type(threshold1))
    else:
        fakeelse=1

    if ifflow > 0 :

        kpisum = 0
        percentual1=0
        percentual2=0
        percentual3=0
        for Indx in range(0, len(Detected)):
            kpisum=kpisum+math.sqrt(2*(Simulated[Indx]-Detected[Indx])*(Simulated[Indx]-Detected[Indx])/(Simulated[Indx]+Detected[Indx]))

            if ( math.sqrt( 2*(Simulated[Indx]-Detected[Indx])**2 / (Simulated[Indx]+Detected[Indx]) ) < threshold1 ):
                percentual1=percentual1+1

            else:
                fake1ifstat=1

    else:
        fakeelse=1

    if len(Strt) > 0 :
        kpisum = kpisum / len(Strt)
        percentual1=percentual1 / len(Strt)
        percentual4= percentual4 / len(Strt)
        percentual5 = percentual5 / len(Strt)
    #else:
    #    kpisum=0
    #    percentual1=0

    if percentual3 > 0 :
        percentual2=percentual2 / percentual3
    else:
        fakeelse=1

    kpi.append(kpisum)
    kpi_ind.append(Inst[0])
    perc1_kpi.append(percentual1)
    perc2_kpi.append(percentual2)
    AA = 'KPI value ' + str(kpisum) + ' INST ' + str(Inst[0]) + ' || % abs dev= '  + str(percentual1*100) + ' || % REL dev= '  + str(percentual4*100) + ' || % abs AND rel= '  + str(percentual5*100) + '|| \n'
    f.write("%s" % AA )
    
    print("")
    print("")
    print("KPI value -- INST")
    print(kpisum,Inst[0],percentual1*100,'abs')
    #print(kpisum,Inst[0],percentual2)
    #print(kpisum,Inst[0],percentual3)	
    print(kpisum,Inst[0],percentual4*100,'rel')
    print(kpisum,Inst[0],percentual5*100,'abs&rel')


print("")
print("DATA Plots (Start).....")
fig = plt.figure('KPI_NET')
plot_title = 'KPI'
plt.title(plot_title)
plt.plot(kpi_ind,kpi,'bo--')
# plt.ylim(0,10)
resultsfileplot='KPI_NET'
completeName = os.path.join(save_path,resultsfileplot)
print(completeName)
fig.savefig(completeName)
print("DATA Plots (End).....")

print("")
print("DATA Plots (Start).....")
fig = plt.figure('PERC_NET')
plot_title = 'PERC'
plt.title(plot_title)
plt.plot(kpi_ind,perc1_kpi,'ro--')
resultsfileplot='PERC_NET'
completeName = os.path.join(save_path,resultsfileplot)
print(completeName)
fig.savefig(completeName)
print("DATA Plots (End).....")

print("")
print("DATA Plots (Start).....")
fig = plt.figure('PERC_THRESHOLD_NET')
plot_title = 'PERC BELOW SPEED %d' %(threshold2)
plt.title(plot_title)
plt.plot(kpi_ind,perc2_kpi,'go--')
resultsfileplot='PERC_THRESHOLD_NET'
completeName = os.path.join(save_path,resultsfileplot)
print(completeName)
fig.savefig(completeName)
print("DATA Plots (End).....")
f.close() 

conn.close()
print("")
print("Connection to OPTIMA database closed")
