#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <list>
#include <vector>
#include <pthread.h>

#define usToms 1000
#define numThreads 10
#define simulationLenghtScheduler 1500
#define numJobs 120
#define maxFails 15

int simulationLenghtThread = 5000;

struct POD
{
    pthread_t thread;
    int id;
    int cpuSpeed;     // in MHz
    int memory;       // in mb
    int networkDelay; // in ms
    int discSpeed;    // in mb/s
};

struct Job
{
    int requiredCycles;
    int requiredMemory;
    int requiredDisc;
    bool completed;
    bool failed;
};

std::vector<POD> pods;
std::list<Job> jobs;
Job *assignedJobs;
pthread_mutex_t mutexAssigned = PTHREAD_MUTEX_INITIALIZER;

long *workTime;
long *waitTime;
int *completedJobs;

int calculaTimeToWork(Job jobAtual, POD podAtual)
{
    int timeToWork = 0;
    timeToWork += jobAtual.requiredCycles / podAtual.cpuSpeed;
    timeToWork += jobAtual.requiredDisc / podAtual.discSpeed;
    timeToWork += podAtual.networkDelay;
    if ((rand() % 10) == 0)
        timeToWork += 20;
    return timeToWork;
}

Job createEmptyJob()
{
    Job jobToReturn;
    jobToReturn.requiredCycles = 0;
    jobToReturn.requiredMemory = 0;
    jobToReturn.requiredDisc = 0;
    jobToReturn.completed = false;
    jobToReturn.failed = false;
    return jobToReturn;
}

void *Trabalha(void *arg)
{
    bool possuiTrabalho = false;
    // POD podAtual;
    POD podAtual = *(POD *)arg;
    printf("thread com id %d iniciada, com %d mhz de CPU, %d mb de memoria, %d ms de delay e %d mb/s de transferencia de memoria \n", podAtual.id, podAtual.cpuSpeed, podAtual.memory, podAtual.networkDelay, podAtual.discSpeed);
    pthread_mutex_lock(&mutexAssigned);
    Job jobAtual = assignedJobs[podAtual.id];
    // printf("ciclos atuais %d \n", jobAtual.requiredCycles);
    pthread_mutex_unlock(&mutexAssigned);
    int timeToWork = 0;
    for (size_t i = 0; i < simulationLenghtThread; i++)
    {
        if (jobAtual.requiredCycles == 0)
        {
            usleep(50 * usToms);
            waitTime[podAtual.id] = waitTime[podAtual.id] + 50;
            pthread_mutex_lock(&mutexAssigned);
            if (!assignedJobs[podAtual.id].completed && !assignedJobs[podAtual.id].failed)
                jobAtual = assignedJobs[podAtual.id];
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
                assignedJobs[podAtual.id] = jobAtual;
                pthread_mutex_unlock(&mutexAssigned);
                jobAtual = createEmptyJob();
            }

            if (!jobAtual.failed)
            {
                timeToWork = calculaTimeToWork(jobAtual, podAtual);
                usleep(timeToWork * usToms);
                jobAtual.completed = true;
                possuiTrabalho = false;
                // printf("Trabalho acabado pela thread %d \n", podAtual.id);
                pthread_mutex_lock(&mutexAssigned);
                assignedJobs[podAtual.id] = jobAtual;
                pthread_mutex_unlock(&mutexAssigned);
                jobAtual = createEmptyJob();

                workTime[podAtual.id] = workTime[podAtual.id] + timeToWork;
                completedJobs[podAtual.id] = completedJobs[podAtual.id] + 1;
            }
        }
    }
    return NULL;
}

POD createRandomPOD(int id)
{
    POD podToReturn;
    podToReturn.id = id;
    podToReturn.cpuSpeed = (rand() % 4000) + 1000;  // Fromm 1-5 GHz
    podToReturn.memory = (rand() % 7000) + 1000;    // 1-8 GB
    podToReturn.networkDelay = (rand() % 990) + 10; // 10-100 ms
    podToReturn.discSpeed = (rand() % 430) + 70;    // 70-500 mb/s
    return podToReturn;
}

Job createRandomJob()
{
    Job jobToReturn;
    jobToReturn.requiredCycles = (rand() % 28000) + 2000;
    jobToReturn.requiredMemory = (rand() % 3200) + 800;
    jobToReturn.requiredDisc = (rand() % 930) + 70;
    jobToReturn.completed = false;
    jobToReturn.failed = false;
    return jobToReturn;
}

bool daParaOPrimeiroDisponivel(Job trabalho)
{
    for (int i = 0; i < numThreads; i++)
    {
        if (assignedJobs[i].requiredCycles == 0)
        {
            assignedJobs[i] = trabalho;
            return true;
        }
    }
    return false;
}

bool daParaOPrimeiroComMem(Job trabalho)
{
    for (int i = 0; i < numThreads; i++)
    {
        if (assignedJobs[i].requiredCycles == 0 && trabalho.requiredMemory < pods[i].memory)
        {
            assignedJobs[i] = trabalho;
            return true;
        }
    }
    return false;
}

bool daParaOComMaisClock(Job trabalho)
{
    int maisRapidoDisponivel = -1;
    int velocidadeDoMaisRapido = 0;
    for (int i = 0; i < numThreads; i++)
    {
        if (assignedJobs[i].requiredCycles == 0 && trabalho.requiredMemory < pods[i].memory)
        {
            if (velocidadeDoMaisRapido < pods[i].cpuSpeed)
            {
                velocidadeDoMaisRapido = pods[i].cpuSpeed;
                maisRapidoDisponivel = i;
            }
        }
    }
    if (maisRapidoDisponivel == -1)
        return false;

    assignedJobs[maisRapidoDisponivel] = trabalho;
    return true;
}

void limpaJobsCompletos()
{
    for (int i = 0; i < numThreads; i++)
    {
        if (assignedJobs[i].completed)
        {
            assignedJobs[i] = createEmptyJob();
            // printf("limpando job completo \n");
        }
    }
}

void limpaJobsCompletosEFalhados()
{
    Job trabalhoFalhado;
    for (int i = 0; i < numThreads; i++)
    {
        if (assignedJobs[i].completed)
        {
            assignedJobs[i] = createEmptyJob();
            // printf("limpando job completo \n");
        }
        else if (assignedJobs[i].failed)
        {
            trabalhoFalhado = assignedJobs[i];
            assignedJobs[i] = createEmptyJob();
            trabalhoFalhado.failed = false;
            jobs.push_front(trabalhoFalhado);
        }
    }
}

bool daParaOMaisRapido(Job trabalho)
{
    int maisRapidoDisponivel = -1;
    int TempoDoMaisRapido = 9999999;
    int tempoDoPodAtual = -1;
    for (int i = 0; i < numThreads; i++)
    {
        if (assignedJobs[i].requiredCycles == 0 && trabalho.requiredMemory < pods[i].memory)
        {
            tempoDoPodAtual = calculaTimeToWork(trabalho, pods[i]);
            if (TempoDoMaisRapido > tempoDoPodAtual)
            {
                TempoDoMaisRapido = tempoDoPodAtual;
                maisRapidoDisponivel = i;
            }
        }
    }
    if (maisRapidoDisponivel == -1)
        return false;

    assignedJobs[maisRapidoDisponivel] = trabalho;
    return true;
}

bool daParaOMaisLento(Job trabalho)
{
    int maisLentoDisponivel = -1;
    int TempoDoMaisLento = -1;
    int tempoDoPodAtual = -1;
    for (int i = 0; i < numThreads; i++)
    {
        if (assignedJobs[i].requiredCycles == 0 && trabalho.requiredMemory < pods[i].memory)
        {
            tempoDoPodAtual = calculaTimeToWork(trabalho, pods[i]);
            if (TempoDoMaisLento < tempoDoPodAtual)
            {
                TempoDoMaisLento = tempoDoPodAtual;
                maisLentoDisponivel = i;
            }
        }
    }
    if (maisLentoDisponivel == -1)
        return false;

    assignedJobs[maisLentoDisponivel] = trabalho;
    return true;
}

POD criaPodMedio() // Em média leva 61ms para completar um job
{
    POD podToReturn;
    podToReturn.id = -1;
    podToReturn.cpuSpeed = 3000;
    podToReturn.memory = 4500;
    podToReturn.networkDelay = 55;
    podToReturn.discSpeed = 285;
    return podToReturn;
}

bool DivideNoMeio(Job trabalho)
{
    int tempoDeTrabalho = calculaTimeToWork(trabalho, criaPodMedio());

    if (tempoDeTrabalho > 61)
        return daParaOMaisRapido(trabalho);
    else
        return daParaOMaisLento(trabalho);
}

int main(void)
{
    srand(time(NULL));
    assignedJobs = new Job[numThreads];
    workTime = new long[numThreads];
    waitTime = new long[numThreads];
    completedJobs = new int[numThreads];
    for (int i = 0; i < numThreads; i++)
    {
        pods.push_back(createRandomPOD(i));
    }
    for (int i = 0; i < numJobs; i++)
    {
        jobs.push_back(createRandomJob());
    }

    for (int i = 0; i < numThreads; i++)
    {
        pthread_create(&(pods[i].thread), NULL, Trabalha, static_cast<void *>(&pods[i]));
    }

    int falhasSeguidas = 0;
    int cycles = 0;
    while (falhasSeguidas < numThreads && jobs.size() > 0 && cycles < simulationLenghtScheduler)
    {
        if (DivideNoMeio(jobs.back()))
        {
            falhasSeguidas = 0;
            usleep(25 * usToms);
        }
        else
        {
            falhasSeguidas++;
            jobs.push_front(jobs.back());
            usleep(15 * usToms);
        }
        jobs.pop_back();
        limpaJobsCompletos();
        cycles++;
    }
    simulationLenghtThread = 0;

    if (falhasSeguidas >= numThreads)
        printf("\nPrograma acabou por alto numero de falhas seguidas\n");
    else if (jobs.size() == 0)
        printf("\nPrograma acabou pois completou todos os jobs foram completos\n");
    else if (cycles >= simulationLenghtScheduler)
        printf("\nPrograma acabou pois acabou os ciclos de scheduler\n");

    // Dando Join
    for (int i = 0; i < numThreads; i++)
    {
        pthread_join((pods[i].thread), NULL);
    }

    printf("Join feitos\n");

    for (int i = 0; i < numThreads; i++)
    {
        printf("A Thread %d completou %d jobs, trabalhando por %ld ms e esperando por %ld \n", i, completedJobs[i], workTime[i], waitTime[i]);
    }

    long trabalhoTotal = 0;
    long esperaTotal = 0;
    for (int i = 0; i < numThreads; i++)
    {
        trabalhoTotal += workTime[i];
        esperaTotal += waitTime[i];
    }
    printf("\nTempo total de trabalho %ld ms \n ", trabalhoTotal);
    printf("Tempo total de espera %ld ms \n ", esperaTotal);

    return 0;
}