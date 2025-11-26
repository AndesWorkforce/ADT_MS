import {
  Catch,
  RpcExceptionFilter as IRpcExceptionFilter,
  ArgumentsHost,
} from '@nestjs/common';
import { RpcException } from '@nestjs/microservices';
import { Observable, throwError } from 'rxjs';

@Catch(RpcException)
export class RpcExceptionFilter implements IRpcExceptionFilter<RpcException> {
  catch(exception: RpcException, _host: ArgumentsHost): Observable<any> {
    const error = exception.getError();

    return throwError(() => {
      if (typeof error === 'object' && error !== null) {
        return {
          status: (error as any).status || 500,
          message: (error as any).message || 'RPC Error',
          custom: (error as any).custom || null,
        };
      } else {
        return {
          status: 500,
          message: typeof error === 'string' ? error : 'RPC Error',
          custom: null,
        };
      }
    });
  }
}

