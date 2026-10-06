// g++ -pthread -o kuber Kubernets.cpp
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <list>
#include <vector>
#include <pthread.h>

#define usToms 1000
#define numWorkers 10
#define simulationLenghtScheduler 1500
#define numPods 120
#define maxFails 15

int simulationLenghtThread = 5000;

struct WORKER
{
    pthread_t thread;
    int id;
    int cpuSpeed;     // in MHz
    int memory;       // in mb
    int networkDelay; // in ms
    int discSpeed;    // in mb/s
};

struct POD
{
    int requiredCycles;
    int requiredMemory;
    int requiredDisc;
    bool completed;
    bool failed;
};

std::vector<WORKER> workers;
std::list<POD> pods;
POD *assignedPods;
pthread_mutex_t mutexAssigned = PTHREAD_MUTEX_INITIALIZER;

long *workTime;
long *waitTime;
int *completedPods;

int calculaTimeToWork(POD jobAtual, WORKER podAtual)
{
    int timeToWork = 0;
    timeToWork += jobAtual.requiredCycles / podAtual.cpuSpeed;
    timeToWork += jobAtual.requiredDisc / podAtual.discSpeed;
    timeToWork += podAtual.networkDelay;
    if ((rand() % 10) == 0)
        timeToWork += 20;
    return timeToWork;
}

POD createEmptyPod()
{
    POD podToReturn;
    podToReturn.requiredCycles = 0;
    podToReturn.requiredMemory = 0;
    podToReturn.requiredDisc = 0;
    podToReturn.completed = false;
    podToReturn.failed = false;
    return podToReturn;
}

void *Trabalha(void *arg)
{
    bool possuiTrabalho = false;
    // POD podAtual;
    WORKER podAtual = *(WORKER *)arg;
    printf("Worker com id %d iniciado, com %d mhz de CPU, %d mb de memoria, %d ms de delay e %d mb/s de transferencia de memoria \n", podAtual.id, podAtual.cpuSpeed, podAtual.memory, podAtual.networkDelay, podAtual.discSpeed);
    pthread_mutex_lock(&mutexAssigned);
    POD jobAtual = assignedPods[podAtual.id];
    pthread_mutex_unlock(&mutexAssigned);
    int timeToWork = 0;
    for (size_t i = 0; i < simulationLenghtThread; i++)
    {
        if (jobAtual.requiredCycles == 0)
        {
            usleep(50 * usToms);
            waitTime[podAtual.id] = waitTime[podAtual.id] + 50;
            pthread_mutex_lock(&mutexAssigned);
            if (!assignedPods[podAtual.id].completed && !assignedPods[podAtual.id].failed)
                jobAtual = assignedPods[podAtual.id];
            pthread_mutex_unlock(&mutexAssigned);
            if (jobAtual.requiredCycles != 0)
                possuiTrabalho = true;
        }
        else
            possuiTrabalho = true;

        if (possuiTrabalho)
        {
            if (jobAtual.requiredMemory > podAtual.memory)
            {
                jobAtual.failed = true;
                // printf("Trabalho falhado pela thread %d \n", podAtual.id);
                pthread_mutex_lock(&mutexAssigned);
                assignedPods[podAtual.id] = jobAtual;
                pthread_mutex_unlock(&mutexAssigned);
                jobAtual = createEmptyPod();
            }

            if (!jobAtual.failed)
            {
                timeToWork = calculaTimeToWork(jobAtual, podAtual);
                usleep(timeToWork * usToms);
                jobAtual.completed = true;
                possuiTrabalho = false;
                // printf("Trabalho acabado pela thread %d \n", podAtual.id);
                pthread_mutex_lock(&mutexAssigned);
                assignedPods[podAtual.id] = jobAtual;
                pthread_mutex_unlock(&mutexAssigned);
                jobAtual = createEmptyPod();

                workTime[podAtual.id] = workTime[podAtual.id] + timeToWork;
                completedPods[podAtual.id] = completedPods[podAtual.id] + 1;
            }
        }
    }
    return NULL;
}

WORKER createRandomWorker(int id)
{
    WORKER podToReturn;
    podToReturn.id = id;
    podToReturn.cpuSpeed = (rand() % 4000) + 1000;  // Fromm 1-5 GHz
    podToReturn.memory = (rand() % 7000) + 1000;    // 1-8 GB
    podToReturn.networkDelay = (rand() % 990) + 10; // 10-100 ms
    podToReturn.discSpeed = (rand() % 430) + 70;    // 70-500 mb/s
    return podToReturn;
}

POD createRandomJob()
{
    POD jobToReturn;
    jobToReturn.requiredCycles = (rand() % 28000) + 2000;
    jobToReturn.requiredMemory = (rand() % 3200) + 800;
    jobToReturn.requiredDisc = (rand() % 930) + 70;
    jobToReturn.completed = false;
    jobToReturn.failed = false;
    return jobToReturn;
}

bool daParaOPrimeiroDisponivel(POD trabalho)
{
    for (int i = 0; i < numWorkers; i++)
    {
        if (assignedPods[i].requiredCycles == 0)
        {
            assignedPods[i] = trabalho;
            return true;
        }
    }
    return false;
}

bool daParaOPrimeiroComMem(POD trabalho)
{
    for (int i = 0; i < numWorkers; i++)
    {
        if (assignedPods[i].requiredCycles == 0 && trabalho.requiredMemory < workers[i].memory)
        {
            assignedPods[i] = trabalho;
            return true;
        }
    }
    return false;
}

bool daParaOComMaisClock(POD trabalho)
{
    int maisRapidoDisponivel = -1;
    int velocidadeDoMaisRapido = 0;
    for (int i = 0; i < numWorkers; i++)
    {
        if (assignedPods[i].requiredCycles == 0 && trabalho.requiredMemory < workers[i].memory)
        {
            if (velocidadeDoMaisRapido < workers[i].cpuSpeed)
            {
                velocidadeDoMaisRapido = workers[i].cpuSpeed;
                maisRapidoDisponivel = i;
            }
        }
    }
    if (maisRapidoDisponivel == -1)
        return false;

    assignedPods[maisRapidoDisponivel] = trabalho;
    return true;
}

void limpaPodsCompletos()
{
    for (int i = 0; i < numWorkers; i++)
    {
        if (assignedPods[i].completed)
        {
            assignedPods[i] = createEmptyPod();
            // printf("limpando job completo \n");
        }
    }
}

void limpaJobsCompletosEFalhados()
{
    POD trabalhoFalhado;
    for (int i = 0; i < numWorkers; i++)
    {
        if (assignedPods[i].completed)
        {
            assignedPods[i] = createEmptyPod();
            // printf("limpando job completo \n");
        }
        else if (assignedPods[i].failed)
        {
            trabalhoFalhado = assignedPods[i];
            assignedPods[i] = createEmptyPod();
            trabalhoFalhado.failed = false;
            pods.push_front(trabalhoFalhado);
        }
    }
}

bool daParaOMaisRapido(POD trabalho)
{
    int maisRapidoDisponivel = -1;
    int TempoDoMaisRapido = 9999999;
    int tempoDoPodAtual = -1;
    for (int i = 0; i < numWorkers; i++)
    {
        if (assignedPods[i].requiredCycles == 0 && trabalho.requiredMemory < workers[i].memory)
        {
            tempoDoPodAtual = calculaTimeToWork(trabalho, workers[i]);
            if (TempoDoMaisRapido > tempoDoPodAtual)
            {
                TempoDoMaisRapido = tempoDoPodAtual;
                maisRapidoDisponivel = i;
            }
        }
    }
    if (maisRapidoDisponivel == -1)
        return false;

    assignedPods[maisRapidoDisponivel] = trabalho;
    return true;
}

bool daParaOMaisLento(POD trabalho)
{
    int maisLentoDisponivel = -1;
    int TempoDoMaisLento = -1;
    int tempoDoPodAtual = -1;
    for (int i = 0; i < numWorkers; i++)
    {
        if (assignedPods[i].requiredCycles == 0 && trabalho.requiredMemory < workers[i].memory)
        {
            tempoDoPodAtual = calculaTimeToWork(trabalho, workers[i]);
            if (TempoDoMaisLento < tempoDoPodAtual)
            {
                TempoDoMaisLento = tempoDoPodAtual;
                maisLentoDisponivel = i;
            }
        }
    }
    if (maisLentoDisponivel == -1)
        return false;

    assignedPods[maisLentoDisponivel] = trabalho;
    return true;
}

WORKER criaWorkerMedio() // Em média leva 61ms para completar um pod
{
    WORKER podToReturn;
    podToReturn.id = -1;
    podToReturn.cpuSpeed = 3000;
    podToReturn.memory = 4500;
    podToReturn.networkDelay = 55;
    podToReturn.discSpeed = 285;
    return podToReturn;
}

bool DivideNoMeio(POD trabalho)
{
    int tempoDeTrabalho = calculaTimeToWork(trabalho, criaWorkerMedio());

    if (tempoDeTrabalho > 61)
        return daParaOMaisRapido(trabalho);
    else
        return daParaOMaisLento(trabalho);
}

int main(void)
{
    srand(time(NULL));
    assignedPods = new POD[numWorkers];
    workTime = new long[numWorkers];
    waitTime = new long[numWorkers];
    completedPods = new int[numWorkers];
    for (int i = 0; i < numWorkers; i++)
    {
        workers.push_back(createRandomWorker(i));
    }
    for (int i = 0; i < numPods; i++)
    {
        pods.push_back(createRandomJob());
    }

    for (int i = 0; i < numWorkers; i++)
    {
        pthread_create(&(workers[i].thread), NULL, Trabalha, static_cast<void *>(&workers[i]));
    }

    int falhasSeguidas = 0;
    int cycles = 0;
    while (falhasSeguidas < numWorkers && pods.size() > 0 && cycles < simulationLenghtScheduler)
    {
        if (DivideNoMeio(pods.back()))
        {
            falhasSeguidas = 0;
            usleep(25 * usToms);
        }
        else
        {
            falhasSeguidas++;
            pods.push_front(pods.back());
            usleep(15 * usToms);
        }
        pods.pop_back();
        limpaPodsCompletos();
        cycles++;
    }
    simulationLenghtThread = 0;

    if (falhasSeguidas >= numWorkers)
        printf("\nPrograma acabou por alto numero de falhas seguidas\n");
    else if (pods.size() == 0)
        printf("\nPrograma acabou pois completou todos os pods foram completos\n");
    else if (cycles >= simulationLenghtScheduler)
        printf("\nPrograma acabou pois acabou os ciclos de scheduler\n");

    // Dando Join
    for (int i = 0; i < numWorkers; i++)
    {
        pthread_join((workers[i].thread), NULL);
    }

    printf("Join feitos\n");

    for (int i = 0; i < numWorkers; i++)
    {
        printf("o Worker %d completou %d pods, trabalhando por %ld ms e esperando por %ld \n", i, completedPods[i], workTime[i], waitTime[i]);
    }

    long trabalhoTotal = 0;
    long esperaTotal = 0;
    for (int i = 0; i < numWorkers; i++)
    {
        trabalhoTotal += workTime[i];
        esperaTotal += waitTime[i];
    }
    printf("\nTempo total de trabalho %ld ms \n ", trabalhoTotal);
    printf("Tempo total de espera %ld ms \n ", esperaTotal);

    return 0;
}