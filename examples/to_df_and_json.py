from polly_python.session import SessionConfiguration
from polly_python.resources.workspaces import Workspaces
from polly_python.models.workspace import Workspace

REFRESH_TOKEN = "eyJjdHkiOiJKV1QiLCJlbmMiOiJBMjU2R0NNIiwiYWxnIjoiUlNBLU9BRVAifQ.ndLpiPUud1yT9K9dTO1ffwLa_IOBMcJ0HafO0GQQwrB3dVsa3Xi4goYxIDxv0qXt4zzWmBc8Fl33XgijrOWKfAdh1ec0RxWM_KQSjMai4Icgc0C62sQdBHWNxh_i53uFcdiypJdWRC8VLH8YhCYwstx4PDie65qLmLgvpEZpJiuCeuCF3eUhlqcxCgPGsd0GM1bY8XqByRDtcEeiAJ8CCpTdeQr91PVb5i6otALZ6L5F71rdNftGSZsC2iIBAZF9MLYvb528yeebiISfRyTA-CYJDL2ZFC37WreLTqIBuSZ3vZ9uljhOsmFY0EKYqLVEaxbuB-Lw7G5z3GmHKLEFFA.EvtrEZ-D_iaK4PLy.bwi_wrNKKPI8lFEqVrrJNdkTqBK1jIi_212kD8zCDywr9vfHKpNQqJeRMGVZUJ-l32ePexTiIbmB0sFw4GM-nlDdbDvKg_Hj6TH_lK5MD6hmCkODugObUHyyEkMAHkfXcbdA7y_9VgU59-9C5qI9tS13PmOKb5qfG6nfuRdAAGab_hBA7WwlUIRAWQb35qAgdQ5DSEBB2uQLsKGv_h5FXOn-H7xC2HDYnNdxnkR6Ma1bIz8NxMCi2AorHnypOBHlHL8em3Gnx5th8aOYSaQyfdFSBRxiADxGobfCIsCnr8PHLf0xn9C-khYxM0_xF1aMaIOko5cLSuUYO_ViyuocEHbU0vt2WpljoUiisIu2_R7qOri7QazGLtqEg_IKoLC0giulrQvCe1BpOvZU1D_3EZNgK2KXawGTpr3I5bfeiZkg-mp_fKUo1vJavch5tV-DAO7-98oBuw0oONJpwH3Hk_c93ADFlfIuYvK-dbRyam3YrngGMd_mT48qla0r0EvAMuSrMNXo0437pGoLl0DDq-DsPGlbG0y8CecIffgiQ15XO0UNK-Jl6jRCIz4jS71SDq4taMpCd26uOirHuoiH2s-g7AGqJ_0Jo6rIEAJcLbv3ZffD-5ajor9lDX9cRd8kLAzIHWfIr4C8hLTEptZ9k1n_9JOmKdE4EzY1bmM8u7E5y9ed8jxzBIXKkQRZSYOlnIpciEG5h9IluTacWCMPrXU4RAvd3Et7E2uqtkDqy8eZ1rXFi3_DR3M-6iTx_Y5OZDFknkZGNF7OidKh2uZBSMeXon5Wuqt2vYFSY5obzpEfNft8aF2XPMB564sBq6M4jKwjkvFRNlfGd5uwghh-lpu7RMXk2otDwaoZ8mAwox1g15en6xn-RqntjjvacckmD0O5Q-wM7nbtQuF5Kg0rw0stUy4XN5QhFUPMc_zrkRYh6kNj85AnTTCPW2imoQ7GfS2aTUjvROIBMZDiiZn0xIWI9v7TH0Vv5n61JrUA4fpYsunbmXpnv-NmuDLBfveocJ1zSzqOEZN75LumtORzn_Pnr5DB-xJWopcxtu2yQcYFvwsEs91l1_q_lCYGL0ree8IiSjdvLazfLJwWUZvZBsV_GDL6zII71q13c6zSHDioUSTAynvHzuaaFIkrbm737tw579AJQqAVpj6OW39-97EyBXpnwnSrIJUWOfTgMrpMyseey5lb3ueXbaFBla3iszoMuHaCGW4-7KsJ85uLsCkaNmmqjINUGUtZzKcWfNhetzUrmA6F-xuwl7feeL9LWLi2Ct1OwbN_Wf7gk02PgL7QzQnZCH3-aKWrJTIwWmSUBriAmBAk3jzP4hU.-NIGSDWH2ibiBmadXgmeQQ"

WORKSPACE_ID = 7237


def main():

    session_config = SessionConfiguration(REFRESH_TOKEN)
    session_config.save()

    workspace_session = Workspaces()

    workspace = workspace_session.get_workspace_by_id(WORKSPACE_ID)
    if type(workspace) == Workspace:
        print("Workspace Object Created")
        print("Workspace Name ----> ", workspace.get_name())

    print("\nAs JSON:")
    workspace_json = workspace.to_json()
    print(workspace_json)

    print("\nAs a DataFrame:")
    workspace_df = workspace.to_df()
    print(workspace_df)


if __name__ == "__main__":
    main()
