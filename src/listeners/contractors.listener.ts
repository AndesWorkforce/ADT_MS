import { Controller, Logger } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';

import { getMessagePattern, logError } from 'config';

import { RawService } from '../raw/raw.service';
import { ContractorRawDto } from '../raw/dto/contractor-raw.dto';
import { ContractorPayload } from 'src/listeners/listener.interfaces';

@Controller()
export class ContractorsListener {
  private readonly logger = new Logger(ContractorsListener.name);

  constructor(private readonly rawService: RawService) {}

  private toContractorRaw(contractor: ContractorPayload): ContractorRawDto {
    return {
      contractor_id: contractor.id,
      name: contractor.name,
      email: contractor.email || null,
      job_position: contractor.job_position,
      work_schedule_start: contractor.work_schedule_start || null,
      work_schedule_end: contractor.work_schedule_end || null,
      country: contractor.country || null,
      client_id: contractor.client_id,
      team_id: contractor.team_id || null,
      isActive: contractor.isActive !== undefined ? contractor.isActive : true,
      created_at: contractor.created_at
        ? new Date(contractor.created_at)
        : new Date(),
      updated_at: contractor.updated_at
        ? new Date(contractor.updated_at)
        : new Date(),
    };
  }

  /**
   * Escuchar evento contractor.created de USER_MS
   */
  @EventPattern(getMessagePattern('contractor.created'))
  async handleContractorCreated(
    @Payload() contractor: ContractorPayload,
  ): Promise<void> {
    try {
      this.logger.debug(`Received contractor.created: ${contractor.id}`);

      const contractorRaw: ContractorRawDto = this.toContractorRaw(contractor);
      await this.rawService.saveContractor(contractorRaw);
      this.logger.debug(
        `✅ Contractor saved to RAW: ${contractorRaw.contractor_id}`,
      );
    } catch (error) {
      logError(this.logger, 'Error processing contractor.created', error);
    }
  }

  /**
   * Escuchar evento contractor.updated de USER_MS
   */
  @EventPattern(getMessagePattern('contractor.updated'))
  async handleContractorUpdated(
    @Payload() contractor: ContractorPayload,
  ): Promise<void> {
    try {
      this.logger.debug(`Received contractor.updated: ${contractor.id}`);

      const contractorRaw: ContractorRawDto = this.toContractorRaw(contractor);
      await this.rawService.saveContractor(contractorRaw);
      this.logger.debug(
        `✅ Contractor updated in RAW: ${contractorRaw.contractor_id}`,
      );
    } catch (error) {
      logError(this.logger, 'Error processing contractor.updated', error);
    }
  }
}
