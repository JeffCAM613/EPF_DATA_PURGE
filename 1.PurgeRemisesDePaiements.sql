------------------------------------------------------ Purge des remises des paiements par date (par défaut on purge les remises de paiements qui ont plus de 30 jours) ------------------------------------------------------------
Declare
 
 nbDays Number := 30;
 dateSuppression Date := Trunc (sysdate - nbDays);
 -- Bulks payments info
 Cursor bulksPaymentsListCursor is
 Select bp.bulk_payment_id
 From oppayments.bulk_payment bp
 Where bp.value_date < dateSuppression
 Order by bp.bulk_payment_id desc;
 
 nBulkPaymentId oppayments.bulk_payment.bulk_payment_id%type;
 
Begin
 
 For bulkPayments in bulksPaymentsListCursor
 Loop
 Begin
 nBulkPaymentId := bulkPayments.bulk_payment_id;
 delete from oppayments.bulk_payment_additional_info where bulk_payment_id=nBulkPaymentId;
 delete from oppayments.payment_audit where bulk_payment_id=nBulkPaymentId;
 delete from oppayments.import_audit where bulk_payment_id=nBulkPaymentId;
 delete from oppayments.transmission_execution_audit where bulk_payment_id=nBulkPaymentId;
 delete from oppayments.transmission_execution where bulk_payment_id=nBulkPaymentId;
 delete from oppayments.transmission_exception where bulk_payment_id=nBulkPaymentId;
 delete from oppayments.notification_execution where bulk_payment_id=nBulkPaymentId;
 delete from oppayments.payment_audit where payment_id in (select payment_id from oppayments.payment where bulk_payment_id=nBulkPaymentId);
 delete from oppayments.approbation_execution where execution_id in (select execution_id from oppayments.workflow_execution where payment_id
 in (select payment_id from oppayments.payment where bulk_payment_id=nBulkPaymentId));
 delete from oppayments.workflow_execution where payment_id in (select payment_id from oppayments.payment where bulk_payment_id=nBulkPaymentId);
 delete from oppayments.payment_additional_info where payment_id in (select payment_id from oppayments.payment where bulk_payment_id=nBulkPaymentId);
 delete from oppayments.payment where bulk_payment_id=nBulkPaymentId;
 delete from oppayments.bulk_payment where bulk_payment_id=nBulkPaymentId;
 commit;
 
 Exception
 When Others Then
 op.SPEC_OUTILS.AddSpecLog('Delete Bulk payments exception. Cannot delete bulk Num: '|| nBulkPaymentId, SQLERRM, '1.PurgeRemisesDePaiements.sql Script');
 End;
 End Loop;
Exception
 When Others Then
 op.SPEC_OUTILS.AddSpecLog('Delete Bulk payments exception. Global Exception', SQLERRM, '1.PurgeRemisesDePaiements.sql Script');
End;
/
------------------------------------------------------ Purge des tables des fichiers/remises de paiements importés en mode Fast Import (par défaut on purge les fichiers/remises de paiements qui ont plus de 30 jours) ------------------------------------------------------------
Declare
 
 nbDays Number := 30;
 dateSuppressionTimeStamp Date := sysdate - nbDays;
 
 -- Files Integration info
 Cursor filesIntergationListCursor is
 Select fi.file_integration_id
 From oppayments.file_integration fi
 Where fi.integration_date < dateSuppressionTimeStamp
 Order by fi.file_integration_id desc;
 
 nFileIntergationId oppayments.file_integration.file_integration_id%type;
 
Begin

 Begin
 For fileIntergation in filesIntergationListCursor
 Loop
 nFileIntergationId := fileIntergation.file_integration_id;
 
 delete from oppayments.file_integration where file_integration_id = nFileIntergationId;
 commit;
 End Loop; 
 Exception
 When Others Then
 op.SPEC_OUTILS.AddSpecLog('Delete File Intergation exception. Cannot delete File Intergation Num: '|| nFileIntergationId, SQLERRM, '1.PurgeRemisesDePaiements.sql Script');
 End;
 
Exception
 When Others Then
 op.SPEC_OUTILS.AddSpecLog('Delete File Intergation exception. Global Exception', SQLERRM, '1.PurgeRemisesDePaiements.sql Script');
End;
/