------------------------------------------------------ Purge des tables des relevés bancaires (par défaut on purge les données qui ont plus de 30 jours) ------------------------------------------------------------
Declare
 
 nbDays Number := 30;
 dateSuppressionTimeStamp Date := sysdate - nbDays;
 
 -- Bank Statments info
 Cursor bankStatmentsListCursor is
 Select fd.file_dispatching_id, dd.directory_dispatching_id
 From oppayments.directory_dispatching dd
 Inner join oppayments.file_dispatching fd on fd.file_dispatching_id = dd.file_dispatching_id
 Where fd.date_reception < dateSuppressionTimeStamp
 Order by fd.file_dispatching_id desc;
 
 nFileDispatchingId oppayments.file_dispatching.file_dispatching_id%type;
 nDirectoryDispatchingId oppayments.directory_dispatching.directory_dispatching_id%type;
 
Begin

 Begin
 For bankStatment in bankStatmentsListCursor
 Loop
 nFileDispatchingId := bankStatment.file_dispatching_id;
 nDirectoryDispatchingId := bankStatment.directory_dispatching_id;
 
 delete from oppayments.directory_dispatching where directory_dispatching_id = nDirectoryDispatchingId;
 delete from oppayments.file_dispatching where file_dispatching_id = nFileDispatchingId;
 commit;
 End Loop; 
 Exception
 When Others Then
 op.SPEC_OUTILS.AddSpecLog('Delete Bank Statment exception. Cannot delete Bank Statment Num: '|| nFileDispatchingId, SQLERRM, '3.PurgeRelevesBancaires.sql Script');
 End;
 
Exception
 When Others Then
 op.SPEC_OUTILS.AddSpecLog('Delete Bank Statment exception. Global Exception', SQLERRM, '3.PurgeRelevesBancaires.sql Script');
End;
/