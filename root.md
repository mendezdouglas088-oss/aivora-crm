```
project-root/
├── src/                                              (API — se queda donde está)
│   ├── main.ts                                            [SIN CAMBIOS]
│   ├── app.module.ts                                      [SIN CAMBIOS]
│   ├── database/entities/
│   │   └── whatsapp-conections.entity.ts                  [MODIFICADO]  (+ columna status)
│   ├── realtime/
│   │   ├── realtime.gateway.ts                            [SIN CAMBIOS]  ← clave, ver abajo
│   │   └── realtime.module.ts                             [SIN CAMBIOS]
│   └── whatsapp/
│       ├── whatsapp.module.ts                             [MODIFICADO]
│       ├── whatsapp.scheduler.ts                          [ELIMINADO]
│       ├── controllers/
│       │   ├── whatsapp.controller.ts                     [MODIFICADO]
│       │   ├── whatsapp-connections.controller.ts         [SIN CAMBIOS]
│       │   └── whatsapp-registered-contacts.controller.ts [SIN CAMBIOS]
│       ├── application/
│       │   └── whatsapp-sync.service.ts                   [MODIFICADO A FONDO]
│       ├── services/
│       │   ├── whatsapp-connections.service.ts            [MODIFICADO]  (+ updateStatus)
│       │   ├── whatsapp-chat.service.ts                   [SIN CAMBIOS]
│       │   ├── whatsapp-message.service.ts                [SIN CAMBIOS]
│       │   ├── whatsapp-group.service.ts                  [SIN CAMBIOS]
│       │   ├── whatsapp-registered-contact.service.ts     [SIN CAMBIOS]
│       │   └── whatsapp-commands.service.ts                [NUEVO]
│       ├── infrastructure/
│       │   ├── jobs/
│       │   │   ├── whatsapp-sync.queue.ts                 [SIN CAMBIOS]
│       │   │   └── whatsapp-commands.queue.ts              [NUEVO]
│       │   ├── producer/
│       │   │   └── whatsapp-sync.processor.ts              [MODIFICADO]
│       │   ├── whatsapp-events.listener.ts                 [MODIFICADO] (se queda solo con 'status')
│       │   ├── whatsapp-persist-events.processor.ts         [NUEVO]  (antes vivía en el listener)
│       │   └── whatsapp-live-events.bridge.ts                [NUEVO]
│       └── domain/
│           └── whatsapp-provider.interface.ts               [ELIMINADO de aquí] → dividido, ver shared/
│
├── runtime/                                           (NUEVO — segundo proceso)
│   └── src/
│       ├── main.ts                                         [NUEVO]
│       ├── runtime.module.ts                               [NUEVO]
│       ├── whatsapp-runtime.service.ts                      [NUEVO]  (antes whatsapp-web.provider.ts)
│       ├── whatsapp-commands.processor.ts                    [NUEVO]
│       └── whatsapp-remote-auth.store.ts                     [NUEVO]
│
├── shared/                                            (NUEVO — contratos compartidos)
│   └── whatsapp-contracts/
│       ├── commands.ts                                      [NUEVO]
│       ├── events.ts                                        [NUEVO]
│       └── types.ts                                         [NUEVO]  (lo que sobrevive de domain/whatsapp-provider.interface.ts)
│
├── tsconfig.json                                       [SIN CAMBIOS]
├── tsconfig.runtime.json                                [NUEVO]
└── package.json                                        [MODIFICADO]  (+script start:runtime)
```
