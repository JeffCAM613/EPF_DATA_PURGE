------------------------------------------------------ Purge des tables de logs et archive(par défaut on purge les données qui ont plus de 30 jours) ------------------------------------------------------------
Declare
 
 nbDays Number := 30;
 dateSuppressionTimeStamp Date := sysdate - nbDays;
 
 -- Audits Trail info
 Cursor auditTrailListCursor is
 Select atr.audit_id, atr.audit_archive_id
 From oppayments.audit_trail atr
 Where atr.audit_timestamp < dateSuppressionTimeStamp
 Order by atr.audit_id desc;
 
 nAuditId oppayments.audit_trail.audit_id%type;
 nAuditArchiveId oppayments.audit_trail.audit_archive_id%type;
 
Begin

 -- 1. Purge des tables ePF d'audit fonctionnelles, les tables d'archive,...
 Begin
 For auditTrail in auditTrailListCursor
 Loop
 nAuditId := auditTrail.audit_id;
 nAuditArchiveId := auditTrail.audit_archive_id;
 
 delete from oppayments.audit_archive where audit_archive_id = nAuditArchiveId;
 delete from oppayments.audit_trail where audit_id = nAuditId;
 commit;
 End Loop; 
 Exception
 When Others Then
 op.SPEC_OUTILS.AddSpecLog('Delete Audit Trail And Archive exception. Cannot delete Audit Num: '|| nAuditId, SQLERRM, '2.PurgeTableLogsEtArchive.sql Script');
 End;
 
 -- 2. Purge de la table ePF des logs techniques
 Begin
 delete from op.spec_trt_log where dtlog < dateSuppressionTimeStamp;
 commit;
 Exception
 When Others Then
 op.SPEC_OUTILS.AddSpecLog('Delete OP SPEC TRT LOG Table exception.', SQLERRM, '2.PurgeTableLogsEtArchive.sql Script');
 End;
 
Exception
 When Others Then
 op.SPEC_OUTILS.AddSpecLog('Delete Log And Archive Tables exception. Global Exception.', SQLERRM, '2.PurgeTableLogsEtArchive.sql Script');
End;
/