import axios from 'axios';
import { v4 as uuidv4, validate as isUuid } from 'uuid';
import { DatabaseService } from './database.service';

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
  async getJobStatus(jobId: string): Promise<{ status: string; progress?: number; message?: string } | null> {
  //   // Implementation needed:
  //   // 1. Validate jobId exists in database
    if (!isUuid(jobId)){
      return null
    }
    // Checking if the job exists
    const exists = await this.getJob(jobId);
    if (!exists) return null;

    // creating object that I will be returning.
    const jobStatus: {
        status: string
        progress?: number,
        message?: string} = { status: ''}

    try {
      // http request on the etlService
      const url = `${this.etlServiceUrl}/jobs/${jobId}/status`
      const { data } = await axios.get(url, {timeout: 5000});
      const status = String(data?.status || '');
      if (!status) return {status: 'failed', message: 'bad etl response'};
      jobStatus.status = data.status
      if (typeof data?.progress === 'number') jobStatus.progress = data.progress;
      if (typeof data?.message === 'string') jobStatus.message = data.message;
    }
    catch(error: any) {
      jobStatus.status = 'failed';
      jobStatus.message = 'Failed to get ETL job status with error: ' + error?.message;
    }
    // TODO: Handle errors more extensively? Revisiting this.

    //   // 3. Handle connection errors gracefully
    return jobStatus;
  }
}
