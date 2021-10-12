import {DatabaseRequest, PlywoodLocator, basicLocator } from 'plywood-base-api';
import { Readable } from 'stream';
import { Client } from 'presto-client-ts';


export interface PrestoRequesterParameters {
    locator?: PlywoodLocator;
    host?: string;
    user?: string;
    source?: string;
    catalog?: string;
}

interface PlywoodRequester<T> {
    (request: DatabaseRequest<T>): Promise<Readable>;
  }

export function prestoRequesterFactory(parameters: PrestoRequesterParameters): PlywoodRequester<string> {
    let locator = parameters.locator;
    if (!locator) {
        let host = parameters.host;
        if (!host) throw new Error("must have a `host` or a `locator`");
        locator = basicLocator(host, 8889);
    }


    let user = !parameters.user ? 'default_user' : parameters.user;
    let source = !parameters.source ? 'default_source' : parameters.source;
    let catalog = !parameters.catalog ? 'hive' : parameters.catalog

    function requestPromise(location: any, query: string): Promise<any> {
        return new Promise((resolve, reject)=>{
            let client = new Client({
                host: location.hostname,
                port: location.port || 8889,
                user: user,
                source: source,
                catalog: catalog
            } as any);

            client.execute({
                query: query,
                data: function (error, data, columns, stats) {
                    resolve(data);
                },
                success: function (error, stats) {
                },
                error: function (error) {
                    reject(error)
                }
            });
        });
    }

    return (request): any => {
        let query = request.query;

        let stream = new Readable({
            objectMode: true,
            read: function () { }
        });

        async function getLocator(){
          const location = await locator();
          const data = await requestPromise(location,query);
          stream.push(data);
          return stream;
        }

        return getLocator();
    };
}