import {
  Entity,
  Column,
  PrimaryGeneratedColumn,
  CreateDateColumn,
  UpdateDateColumn,
  ManyToOne,
  JoinColumn,
  OneToMany,
} from 'typeorm';
import { User } from './user.entity';
import { WhatsappGroup } from './whatsapp-group.entity';
import { WhatsappConnectionStatus } from 'shared/whatsapp-contracts';

@Entity('whatsapp_connections')
export class WhatsappConnections {
  @PrimaryGeneratedColumn('uuid')
  id: string;

  @Column()
  nameUserConnected: string;

  @ManyToOne(() => User, (user) => user.whatsappConnections, {
    onDelete: 'SET NULL',
    nullable: true,
  })
  @JoinColumn({ name: 'userId' })
  user: User;

  @Column({ nullable: true })
  userId: string;

  // este sera el id de la conexion de whatsapp, que se genera en el backend y se
  // envia al frontend para que el usuario pueda escanear el QR y conectarse a su cuenta de whatsapp
  @Column({ nullable: true, unique: true })
  connectionId: string;

  @Column({ type: 'varchar', default: 'disconnected' })
  status: WhatsappConnectionStatus;

  @OneToMany(
    () => WhatsappGroup,
    (whatsappGroup) => whatsappGroup.whatsappConnection,
    {
      cascade: true,
      onDelete: 'CASCADE',
    },
  )
  whatsappGroups: WhatsappGroup[];

  @CreateDateColumn()
  createdAt: Date;

  @UpdateDateColumn()
  updatedAt: Date;
}
