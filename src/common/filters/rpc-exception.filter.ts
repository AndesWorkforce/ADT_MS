import { Catch, Logger } from '@nestjs/common';
import { RpcException } from '@nestjs/microservices';
import { Observable, throwError } from 'rxjs';

@Catch()
export class RpcExceptionFilter {
  private readonly logger = new Logger(RpcExceptionFilter.name);

  catch(exception: any): Observable<any> {
    if (exception instanceof RpcException) {
      const error = exception.getError();
      return throwError(() => {
        if (typeof error === 'object' && error !== null) {
          return {
            status: (error as any).status || 500,
            message: (error as any).message || 'RPC Error',
            custom: (error as any).custom || null,
          };
        }
        return {
          status: 500,
          message: typeof error === 'string' ? error : 'RPC Error',
          custom: null,
        };
      });
    }

    const message =
      exception instanceof Error ? exception.message : String(exception);

    this.logger.error(
      `Unhandled exception in NATS handler: ${message}`,
      exception?.stack,
    );

    return throwError(() => ({
      status: 500,
      message,
      custom: null,
    }));
  }
}
