import axios from 'axios';
import { v4 as uuidv4, validate as isUuid } from 'uuid';
import { DatabaseService } from './database.service';

type JobStatus = {
  jobId: string;
  status: string;
  progress?: number;
  message?: string;
};

export interface ETLJob {
  id: string;
  filename: string;
  studyId?: string;
  status: 'pending' | 'running' | 'completed' | 'failed';
  createdAt: Date;
  updatedAt: Date;
  completedAt?: Date;
  errorMessage?: string;
}

export class ETLService {
  private dbService: DatabaseService;
  private etlServiceUrl: string;

  constructor() {
    this.dbService = new DatabaseService();
    this.etlServiceUrl = process.env.ETL_SERVICE_URL || 'http://etl:8000';
  }

  /**
   * Submit new ETL job
   */
  async submitJob(filename: string, studyId?: string): Promise<ETLJob> {
    const jobId = uuidv4();
    
    // Create job record in database
    const job: ETLJob = {
      id: jobId,
      filename,
      studyId,
      status: 'pending',
      createdAt: new Date(),
      updatedAt: new Date()
    };

    await this.dbService.createETLJob(job);

    // Submit job to ETL service
    try {
      await axios.post(`${this.etlServiceUrl}/jobs`, {
        jobId,
        filename,
        studyId
      });

      // Update job status to running
      await this.dbService.updateETLJobStatus(jobId, 'running');
      job.status = 'running';
      job.errorMessage = undefined;
    } catch (error) {
      // Update job status to failed
      job.status = 'failed';
      job.errorMessage = 'Failed to submit to ETL service';
      await this.dbService.updateETLJobStatus(jobId, job.status, job.errorMessage);
    }

    return job;
  }

  /**
   * Get ETL job by ID
   */
  async getJob(jobId: string): Promise<ETLJob | null> {
    return await this.dbService.getETLJob(jobId);
  }

  // TODO: CANDIDATE TO IMPLEMENT
  // /**
  //  * Get ETL job status from ETL service
  //  */
  async getJobStatus(jobId: string): Promise<JobStatus | null> {
  //   // Implementation needed:
  //   // 1. Validate jobId exists in database
    if (!isUuid(jobId)){
      return null
    }
    // Checking if the job exists
    const exists = await this.getJob(jobId);
    if (!exists) return null;

    try {
      // http request on the etlService
      const url = `${this.etlServiceUrl}/jobs/${jobId}/status`
      const { data } = await axios.get(url, {timeout: 5000});

      // Quick checks
      const status = String(data?.status || '');
      if (!status){
        return {jobId, status: 'failed', message: 'bad etl response'};
      }
      if (!(typeof data?.progress === 'number')) {
        return {jobId, status: 'failed', message: 'invalid progress data'};
      }
      if (!(typeof data?.message === 'string')){
        return {jobId, status: 'failed', message: 'invalid message data'};
      }

      return {
        jobId,
        status,
        progress: data?.progress,
        message: data?.message,
      }

    }
    catch(error: any) {
      return {
        jobId,
        status: "Failed",
        message: "Failed to get ETL job status with error: " + error?.message,
      }
    }
    // TODO: Handle errors more extensively? Revisiting this.

    //   // 3. Handle connection errors gracefully
  }
}
